/**
 * FuelLink MQTT Bridge Server
 * ===========================
 * Connects to iot.gorex.ai:1883 and provides WebSocket for the dashboard
 */
const net        = require("net");
const http       = require("http");
const WebSocket  = require("ws");
const mqttPacket = require("mqtt-packet");

const PORT_WS   = process.env.PORT || 3001;
const MQTT_HOST = process.env.MQTT_HOST || "iot.gorex.ai";
const MQTT_PORT = parseInt(process.env.MQTT_PORT || "1883");
const MQTT_USER = process.env.MQTT_USER || "fuelingnozzle";
const MQTT_PASS = process.env.MQTT_PASS || "FuelingNozzle@fai";

const devices   = {};
const events    = [];
const wsClients = new Set();
let mqttSocket    = null;
let mqttConnected = false;

function broadcast(data) {
  const msg = JSON.stringify(data);
  wsClients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

function sendMQTT(pkt) {
  if (!mqttSocket || !mqttConnected) return false;
  try { mqttSocket.write(mqttPacket.generate(pkt)); return true; }
  catch(e) { return false; }
}

function connectMQTT() {
  console.log(`[MQTT] Connecting to ${MQTT_HOST}:${MQTT_PORT}...`);
  mqttSocket = net.createConnection(MQTT_PORT, MQTT_HOST);

  mqttSocket.on("connect", () => {
    console.log(`[MQTT] TCP connected`);
    mqttSocket.write(mqttPacket.generate({
      cmd: "connect", protocolId: "MQTT", protocolVersion: 4,
      clientId: "fuellink_bridge_" + Date.now().toString(36),
      username: MQTT_USER, password: Buffer.from(MQTT_PASS),
      keepalive: 30, clean: true,
    }));
  });

  const parser = mqttPacket.parser({ protocolVersion: 4 });

  parser.on("packet", pkt => {
    if (pkt.cmd === "connack") {
      if (pkt.returnCode === 0) {
        mqttConnected = true;
        console.log("[MQTT] ✓ Broker connected");
        sendMQTT({ cmd: "subscribe", messageId: 1, subscriptions: [{ topic: "fueling/#", qos: 0 }] });
        broadcast({ type: "broker_connected", host: MQTT_HOST });
      } else {
        console.error("[MQTT] CONNACK error:", pkt.returnCode);
      }
    }

    if (pkt.cmd === "publish") {
      const topic   = pkt.topic;
      const payload = pkt.payload.toString();
      console.log(`[↓] ${topic} → ${payload.substring(0, 100)}`);

      const parts = topic.split("/");
      if (parts[0] !== "fueling" || parts.length < 3) return;
      const deviceId  = parts[1];
      const topicType = parts[2];

      if (!devices[deviceId]) {
        devices[deviceId] = { deviceId, firstSeen: new Date().toISOString(), topics: [], msgCount: 0 };
        console.log(`[NEW DEVICE] ${deviceId}`);
      }
      const dev = devices[deviceId];
      dev.lastSeen = new Date().toISOString();
      dev.msgCount++;
      if (!dev.topics.includes(topicType)) dev.topics.push(topicType);

      try {
        const data = JSON.parse(payload);
        if (topicType === "status") Object.assign(dev, { status: data.status, battery: data.battery, firmware: data.firmware, hardware: data.hardware });
        if (topicType === "event") { events.unshift({ ...data, deviceId, receivedAt: new Date().toISOString() }); if (events.length > 500) events.length = 500; }
        if (topicType === "auth") {
          sendMQTT({ cmd: "publish", topic: `fueling/${deviceId}/auth`, qos: 0, retain: false, dup: false,
            payload: Buffer.from(JSON.stringify({ deviceId, vehicleId: data.vehicleId, authStatus: "success", timestamp: new Date().toISOString(), qtyAllowed: "50L" })) });
          console.log(`[AUTH] approved → ${deviceId}`);
        }
      } catch(e) {}

      broadcast({ type: "mqtt_message", topic, payload, deviceId, topicType, device: devices[deviceId] });
    }

    if (pkt.cmd === "pingresp") {}
  });

  parser.on("error", () => {});
  mqttSocket.on("data", d => parser.parse(d));

  const pingTimer = setInterval(() => { if (mqttConnected) sendMQTT({ cmd: "pingreq" }); }, 25000);

  mqttSocket.on("close", () => {
    mqttConnected = false;
    clearInterval(pingTimer);
    console.log("[MQTT] Disconnected — reconnecting in 5s...");
    broadcast({ type: "broker_disconnected" });
    setTimeout(connectMQTT, 5000);
  });

  mqttSocket.on("error", e => { console.error("[MQTT] Error:", e.message); mqttConnected = false; clearInterval(pingTimer); });
}

const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");
  const url = req.url.split("?")[0];
  if (url === "/health")  return res.end(JSON.stringify({ ok: true, mqttConnected, broker: `${MQTT_HOST}:${MQTT_PORT}`, deviceCount: Object.keys(devices).length, devices: Object.keys(devices), eventCount: events.length, uptime: Math.floor(process.uptime()) + "s", wsClients: wsClients.size }, null, 2));
  if (url === "/devices") return res.end(JSON.stringify(devices, null, 2));
  if (url === "/events")  return res.end(JSON.stringify(events.slice(0, 50), null, 2));
  res.end(JSON.stringify({ ok: true, name: "FuelLink MQTT Bridge", endpoints: ["/health", "/devices", "/events"] }));
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", ws => {
  wsClients.add(ws);
  console.log(`[WS] Client connected (${wsClients.size} total)`);
  ws.send(JSON.stringify({ type: "init", mqttConnected, devices, events: events.slice(0, 20) }));

  ws.on("message", data => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === "publish" && msg.topic) {
        const pl = typeof msg.payload === "string" ? msg.payload : JSON.stringify(msg.payload);
        const sent = sendMQTT({ cmd: "publish", topic: msg.topic, payload: Buffer.from(pl), qos: msg.qos ?? 1, retain: false, dup: false, messageId: Math.floor(Math.random() * 65535) });
        console.log(`[↑] ${msg.topic} → ${sent ? "sent" : "failed"}`);
        ws.send(JSON.stringify({ type: "publish_ack", topic: msg.topic, sent }));
      }
      if (msg.type === "get_devices") ws.send(JSON.stringify({ type: "devices", devices }));
    } catch(e) {}
  });

  ws.on("close", () => wsClients.delete(ws));
  ws.on("error", () => {});
});

httpServer.listen(PORT_WS, "0.0.0.0", () => {
  console.log(`\n╔══════════════════════════════════════╗`);
  console.log(`║  FuelLink MQTT Bridge — Running      ║`);
  console.log(`║  HTTP + WebSocket : port ${PORT_WS}        ║`);
  console.log(`║  MQTT Broker      : ${MQTT_HOST} ║`);
  console.log(`╚══════════════════════════════════════╝\n`);
  connectMQTT();
});

process.on("SIGINT",  () => { if (mqttSocket) mqttSocket.destroy(); process.exit(0); });
process.on("SIGTERM", () => { if (mqttSocket) mqttSocket.destroy(); process.exit(0); });