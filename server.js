// MQTT → FuelLink HTTPS Bridge
import mqtt from "mqtt";
import crypto from "node:crypto";

function getEnvVars() {
  const vars = {
    MQTT_URL: process.env.MQTT_URL,
    MQTT_USERNAME: process.env.MQTT_USERNAME,
    MQTT_PASSWORD: process.env.MQTT_PASSWORD,
    MQTT_CLIENT_ID: process.env.MQTT_CLIENT_ID || `fuellink-bridge-${crypto.randomBytes(4).toString("hex")}`,
    BACKEND_URL: process.env.BACKEND_URL,
    NOZZLE_BRIDGE_TOKEN: process.env.NOZZLE_BRIDGE_TOKEN,
    TELEMETRY_TOPIC: process.env.TELEMETRY_TOPIC || "fueling/+/status",
    EVENT_TOPIC: process.env.EVENT_TOPIC || "fueling/+/event",
    AUTH_TOPIC: process.env.AUTH_TOPIC || "fueling/+/auth",
    OFFLINE_AFTER_SECONDS: process.env.OFFLINE_AFTER_SECONDS || "180",
    IDLE_PING_SECONDS: process.env.IDLE_PING_SECONDS || "120",
  };

  const missing = [];
  if (!vars.MQTT_URL) missing.push("MQTT_URL");
  if (!vars.BACKEND_URL) missing.push("BACKEND_URL");
  if (!vars.NOZZLE_BRIDGE_TOKEN) missing.push("NOZZLE_BRIDGE_TOKEN");

  if (missing.length > 0) {
    console.error(`[ERROR] Missing required env vars: ${missing.join(", ")}`);
    console.error(`[DEBUG] Available env vars: ${Object.keys(process.env).filter(k => k.startsWith("MQTT_") || k.startsWith("BACKEND_") || k.startsWith("NOZZLE_")).join(", ")}`);
    process.exit(1);
  }

  return vars;
}

const { MQTT_URL, MQTT_USERNAME, MQTT_PASSWORD, MQTT_CLIENT_ID, BACKEND_URL, NOZZLE_BRIDGE_TOKEN, TELEMETRY_TOPIC, EVENT_TOPIC, AUTH_TOPIC, OFFLINE_AFTER_SECONDS, IDLE_PING_SECONDS } = getEnvVars();

const idleMs = Number(IDLE_PING_SECONDS) * 1000;
const offlineMs = Number(OFFLINE_AFTER_SECONDS) * 1000;
const lastSeenByDevice = new Map();
const onlineDevices = new Set();

function deviceIdFromTopic(topic) {
  const parts = topic.split("/");
  if (parts.length >= 3 && parts[0] === "fueling") return parts[1];
  return null;
}

async function post(path, body, label) {
  try {
    const res = await fetch(`${BACKEND_URL}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", "x-bridge-token": NOZZLE_BRIDGE_TOKEN },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      console.warn(`[${label}] → ${res.status} ${txt.slice(0, 200)}`);
    } else {
      console.log(`[${label}] → 200`);
    }
  } catch (e) {
    console.warn(`[${label}] failed:`, e.message);
  }
}

function postPresence(deviceId, event, extra = {}) {
  if (!deviceId) return;
  return post("/api/public/nozzle-presence",
    { device_id: deviceId, event, ts: new Date().toISOString(), ...extra },
    `presence ${event} ${deviceId}`);
}

function postTelemetry(deviceId, payload, topic) {
  if (!deviceId) return;
  const body = {
    device_id: deviceId, topic,
    ...(payload && typeof payload === "object" ? payload : { raw: String(payload) }),
  };
  return post("/api/public/nozzle-telemetry", body, `telemetry ${deviceId}`);
}

function markOnline(deviceId) {
  if (!deviceId) return;
  lastSeenByDevice.set(deviceId, Date.now());
  if (!onlineDevices.has(deviceId)) {
    onlineDevices.add(deviceId);
    postPresence(deviceId, "connected");
  }
}

const client = mqtt.connect(MQTT_URL, {
  clientId: MQTT_CLIENT_ID,
  username: MQTT_USERNAME || undefined,
  password: MQTT_PASSWORD || undefined,
  reconnectPeriod: 5000,
  keepalive: 30,
  clean: true,
  protocolVersion: 4,
});

client.on("connect", () => {
  console.log(`[mqtt] connected as ${MQTT_CLIENT_ID} to ${MQTT_URL}`);
  const subs = [TELEMETRY_TOPIC, EVENT_TOPIC, AUTH_TOPIC];
  client.subscribe(subs, { qos: 0 }, (err, granted) => {
    if (err) console.error("[mqtt] subscribe error:", err.message);
    else console.log("[mqtt] subscribed:", granted.map(g => `${g.topic} (qos ${g.qos})`).join(", "));
  });
});

client.on("reconnect", () => console.log("[mqtt] reconnecting..."));
client.on("close", () => console.log("[mqtt] connection closed"));
client.on("error", (e) => console.error("[mqtt] error:", e.message));

client.on("message", (topic, message) => {
  try {
    const deviceId = deviceIdFromTopic(topic);
    if (!deviceId) return;
    let payload = null;
    try { payload = JSON.parse(message.toString("utf-8")); }
    catch { payload = { raw: message.toString("utf-8") }; }
    markOnline(deviceId);
    postTelemetry(deviceId, payload, topic);
  } catch (e) {
    console.error("[message] handler error:", e.message);
  }
});

setInterval(() => {
  const now = Date.now();
  for (const [deviceId, ts] of lastSeenByDevice.entries()) {
    const silent = now - ts;
    if (silent >= offlineMs) {
      if (onlineDevices.has(deviceId)) {
        onlineDevices.delete(deviceId);
        postPresence(deviceId, "disconnected", { reason: "idle_timeout" });
      }
      lastSeenByDevice.delete(deviceId);
    } else if (silent >= idleMs) {
      postPresence(deviceId, "ping");
      lastSeenByDevice.set(deviceId, now);
    }
  }
}, Math.max(15_000, Math.min(idleMs, offlineMs) / 2));

process.on("SIGINT", () => {
  console.log("Shutting down...");
  client.end(true, () => process.exit(0));
});