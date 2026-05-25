const net = require("net");
const http = require("http");
const WebSocket = require("ws");
const mqttPacket = require("mqtt-packet");

const VERSION = "3.0";
const NOZZLE_TOPICS = ["status", "event", "auth", "telemetry"];
const PORT = process.env.PORT || 3001;
const MQTT_HOST = "iot.gorex.ai";
const MQTT_PORT = 1883;
const MQTT_USER = "fuelingnozzle";
const MQTT_PASS = "FuelingNozzle@fai";
const OFFLINE_TIMEOUT_MS = 90 * 1000;

const devices = {};
const events = [];
const wsClients = new Set();
let mqttSocket = null;
let mqttConnected = false;

// ── Helpers ──────────────────────────────────────────────────────────────────

function broadcast(data) {
  const msg = JSON.stringify(data);
  wsClients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

function sendMQTT(pkt) {
  if (!mqttSocket || !mqttConnected) return false;
  try { mqttSocket.write(mqttPacket.generate(pkt)); return true; }
  catch (e) { console.error("[MQTT] send error:", e.message); return false; }
}

/** Mark a device offline and broadcast the state change. */
function markOffline(deviceId) {
  const dev = devices[deviceId];
  if (!dev || dev.status === "offline") return;
  dev.status = "offline";
  console.log(`[OFFLINE] Device ${deviceId} marked offline after ${OFFLINE_TIMEOUT_MS / 1000}s of inactivity`);
  broadcast({ type: "device_offline", deviceId, device: getCleanDevice(dev) });
}

/** Reset (or start) the 90-second offline timer for a device. */
function resetOfflineTimer(deviceId) {
  const dev = devices[deviceId];
  if (!dev) return;
  if (dev._offlineTimer) clearTimeout(dev._offlineTimer);
  dev._offlineTimer = setTimeout(() => markOffline(deviceId), OFFLINE_TIMEOUT_MS);
}

/** Return a device object safe to serialise (no internal timer handle). */
function getCleanDevice(dev) {
  const { _offlineTimer, ...clean } = dev;
  return clean;
}

/** Return all devices as a plain object, timers stripped. */
function getCleanDevices() {
  const out = {};
  for (const [id, dev] of Object.entries(devices)) out[id] = getCleanDevice(dev);
  return out;
}

// ── MQTT ─────────────────────────────────────────────────────────────────────

function connectMQTT() {
  console.log(`[MQTT] Connecting to ${MQTT_HOST}:${MQTT_PORT}...`);
  mqttSocket = net.createConnection(MQTT_PORT, MQTT_HOST);

  mqttSocket.on("connect", () => {
    const clientId = "fl_" + Date.now().toString(36);
    console.log(`[MQTT] TCP connected — sending CONNECT (clientId: ${clientId})`);
    mqttSocket.write(mqttPacket.generate({
      cmd: "connect", protocolId: "MQTT", protocolVersion: 4,
      clientId, username: MQTT_USER,
      password: Buffer.from(MQTT_PASS),
      keepalive: 60, clean: true
    }));
  });

  const parser = mqttPacket.parser({ protocolVersion: 4 });

  parser.on("packet", pkt => {
    // ── CONNACK ──
    if (pkt.cmd === "connack") {
      if (pkt.returnCode === 0) {
        mqttConnected = true;
        console.log("[MQTT] Broker accepted connection (CONNACK 0)");
        sendMQTT({
          cmd: "subscribe", messageId: 1,
          subscriptions: [{ topic: "fueling/#", qos: 0 }]
        });
        console.log("[MQTT] Subscribed to fueling/#");
        broadcast({ type: "broker_connected" });
      } else {
        console.error(`[MQTT] Broker rejected connection (CONNACK ${pkt.returnCode})`);
      }
    }

    // ── PUBLISH ──
    if (pkt.cmd === "publish") {
      const topic = pkt.topic;
      const payload = pkt.payload.toString();
      const parts = topic.split("/");
      if (parts[0] !== "fueling" || parts.length < 3) return;

      const deviceId = parts[1];
      const topicType = parts[2];

      // Initialise device record on first sight
      if (!devices[deviceId]) {
        devices[deviceId] = {
          deviceId,
          status: "unknown",
          msgCount: 0,
          topics: [],
          firstSeen: new Date().toISOString(),
          battery: null,
          firmware: null,
          hardware: null,
          name: null
        };
        console.log(`[up] New device discovered: ${deviceId}`);
      }

      const dev = devices[deviceId];
      const isFromNozzle = NOZZLE_TOPICS.includes(topicType);

      if (!isFromNozzle) {
        console.log(`[CMD-ECHO] Ignoring command echo on topic ${topic} — not counting as nozzle activity`);
      }

      const wasOffline = dev.status === "offline";
      dev.lastSeen = new Date().toISOString();
      dev.msgCount++;
      if (!dev.topics.includes(topicType)) dev.topics.push(topicType);

      // Only reset the offline watchdog for actual nozzle messages
      if (isFromNozzle) {
        dev.lastNozzleMessage = new Date().toISOString();
        resetOfflineTimer(deviceId);

        // Mark back online if it was previously offline
        if (wasOffline) {
          dev.status = "online";
          console.log(`[up] Device ${deviceId} is back online`);
          broadcast({ type: "device_online", deviceId, device: getCleanDevice(dev) });
        }
      }

      try {
        const data = JSON.parse(payload);

        if (topicType === "status") {
          const prevStatus = dev.status;
          Object.assign(dev, {
            status: data.status || "online",
            battery: data.battery ?? dev.battery,
            firmware: data.firmware ?? dev.firmware,
            hardware: data.hardware ?? dev.hardware,
            name: data.name ?? dev.name
          });
          console.log(`[down] Status from ${deviceId}: status=${dev.status} battery=${dev.battery} firmware=${dev.firmware}`);
          if (prevStatus !== dev.status) {
            console.log(`[down] Device ${deviceId} status changed: ${prevStatus} → ${dev.status}`);
          }
        }

        if (topicType === "event") {
          const evt = { ...data, deviceId, receivedAt: new Date().toISOString() };
          events.unshift(evt);
          if (events.length > 500) events.length = 500;
          console.log(`[EVENT] ${deviceId} → ${JSON.stringify(data)}`);
        }

        if (topicType === "auth") {
          console.log(`[AUTH] Auth request from ${deviceId} for vehicleId=${data.vehicleId}`);
          const authResponse = {
            deviceId,
            vehicleId: data.vehicleId,
            authStatus: "success",
            timestamp: new Date().toISOString(),
            qtyAllowed: "50L"
          };
          sendMQTT({
            cmd: "publish",
            topic: "fueling/" + deviceId + "/auth",
            payload: Buffer.from(JSON.stringify(authResponse)),
            qos: 0, retain: false, dup: false
          });
          console.log(`[AUTH] Auth response sent to ${deviceId}: authStatus=success`);
        }
      } catch (e) {
        console.warn(`[MQTT] Could not parse payload on ${topic}: ${e.message}`);
      }

      broadcast({
        type: "mqtt_message", topic, payload, deviceId, topicType,
        device: getCleanDevice(dev)
      });
    }

    if (pkt.cmd === "pingresp") {
      // keepalive acknowledged — no-op
    }
  });

  parser.on("error", e => console.error("[MQTT] Parser error:", e.message));
  mqttSocket.on("data", d => parser.parse(d));

  const ping = setInterval(() => {
    if (mqttConnected) sendMQTT({ cmd: "pingreq" });
  }, 25000);

  mqttSocket.on("close", () => {
    mqttConnected = false;
    clearInterval(ping);
    console.log("[MQTT] Disconnected from broker — reconnecting in 8s");
    broadcast({ type: "broker_disconnected" });
    setTimeout(connectMQTT, 8000);
  });

  mqttSocket.on("error", e => {
    console.error("[MQTT] Socket error:", e.message);
    mqttConnected = false;
    clearInterval(ping);
  });
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");
  const url = req.url.split("?")[0];

  if (url === "/health") {
    const clean = getCleanDevices();
    const onlineCount = Object.values(clean).filter(d => d.status === "online").length;
    const offlineCount = Object.values(clean).filter(d => d.status === "offline").length;
    return res.end(JSON.stringify({
      ok: true,
      version: VERSION,
      broker: `${MQTT_HOST}:${MQTT_PORT}`,
      mqttConnected,
      deviceCount: Object.keys(clean).length,
      onlineCount,
      offlineCount,
      offlineTimeoutSeconds: OFFLINE_TIMEOUT_MS / 1000,
      devices: Object.keys(clean),
      eventCount: events.length,
      uptime: Math.floor(process.uptime()) + "s"
    }));
  }

  if (url === "/devices") return res.end(JSON.stringify(getCleanDevices(), null, 2));
  if (url === "/events") return res.end(JSON.stringify(events.slice(0, 50), null, 2));

  res.end(JSON.stringify({ ok: true, name: "FuelLink Bridge", version: VERSION }));
});

// ── WebSocket ─────────────────────────────────────────────────────────────────

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", (ws, req) => {
  const clientIp = req.headers["x-forwarded-for"] || req.socket.remoteAddress;
  console.log(`[WS] Client connected from ${clientIp} (total: ${wsClients.size + 1})`);
  wsClients.add(ws);

  ws.send(JSON.stringify({
    type: "init",
    version: VERSION,
    mqttConnected,
    devices: getCleanDevices(),
    events: events.slice(0, 20)
  }));

  ws.on("message", data => {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.type === "publish" && msg.topic) {
        const pl = typeof msg.payload === "string" ? msg.payload : JSON.stringify(msg.payload);
        sendMQTT({
          cmd: "publish", topic: msg.topic, payload: Buffer.from(pl),
          qos: 1, retain: false, dup: false,
          messageId: Math.floor(Math.random() * 65535)
        });
        console.log(`[WS] Client publish → ${msg.topic}`);
      }

      if (msg.type === "get_devices") {
        ws.send(JSON.stringify({ type: "devices", devices: getCleanDevices() }));
      }
    } catch (e) {
      console.warn("[WS] Could not parse client message:", e.message);
    }
  });

  ws.on("close", () => {
    wsClients.delete(ws);
    console.log(`[WS] Client disconnected from ${clientIp} (total: ${wsClients.size})`);
  });

  ws.on("error", e => console.error(`[WS] Client error (${clientIp}):`, e.message));
});

// ── Boot ──────────────────────────────────────────────────────────────────────

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log(`[up] FuelLink Bridge v${VERSION} running on port ${PORT}`);
  console.log(`[up] Offline timeout: ${OFFLINE_TIMEOUT_MS / 1000}s`);
  console.log("FIX: Only status/event/auth topics count as nozzle activity");
  connectMQTT();
});
