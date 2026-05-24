const net = require("net");
const http = require("http");
const WebSocket = require("ws");
const mqttPacket = require("mqtt-packet");

const PORT = process.env.PORT || 3001;
const MQTT_HOST = "iot.gorex.ai";
const MQTT_PORT = 1883;
const MQTT_USER = "fuelingnozzle";
const MQTT_PASS = "FuelingNozzle@fai";

const devices = {};
const events = [];
const wsClients = new Set();
let mqttSocket = null;
let mqttConnected = false;

function broadcast(data) {
  const msg = JSON.stringify(data);
  wsClients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

function sendMQTT(pkt) {
  if (!mqttSocket || !mqttConnected) return false;
  try { mqttSocket.write(mqttPacket.generate(pkt)); return true; }
  catch(e) { return false; }
}

function connectMQTT() {
  console.log("Connecting to MQTT broker...");
  mqttSocket = net.createConnection(MQTT_PORT, MQTT_HOST);
  
  mqttSocket.on("connect", () => {
    const clientId = "fl_" + Date.now().toString(36);
    mqttSocket.write(mqttPacket.generate({
      cmd: "connect", protocolId: "MQTT", protocolVersion: 4,
      clientId, username: MQTT_USER,
      password: Buffer.from(MQTT_PASS),
      keepalive: 60, clean: true
    }));
  });
  
  const parser = mqttPacket.parser({ protocolVersion: 4 });
  
  parser.on("packet", pkt => {
    if (pkt.cmd === "connack") {
      if (pkt.returnCode === 0) {
        mqttConnected = true;
        console.log("MQTT connected successfully");
        sendMQTT({ cmd: "subscribe", messageId: 1,
          subscriptions: [{ topic: "fueling/#", qos: 0 }] });
        broadcast({ type: "broker_connected" });
      }
    }
    
    if (pkt.cmd === "publish") {
      const topic = pkt.topic;
      const payload = pkt.payload.toString();
      const parts = topic.split("/");
      if (parts[0] !== "fueling" || parts.length < 3) return;
      
      const deviceId = parts[1];
      const topicType = parts[2];
      
      if (!devices[deviceId]) devices[deviceId] = { deviceId, msgCount: 0, topics: [] };
      const dev = devices[deviceId];
      dev.lastSeen = new Date().toISOString();
      dev.msgCount++;
      if (!dev.topics.includes(topicType)) dev.topics.push(topicType);
      
      try {
        const data = JSON.parse(payload);
        if (topicType === "status") Object.assign(dev, { status: data.status, battery: data.battery, firmware: data.firmware });
        if (topicType === "event") { events.unshift({ ...data, deviceId, receivedAt: new Date().toISOString() }); if (events.length > 500) events.length = 500; }
        if (topicType === "auth") {
          sendMQTT({ cmd: "publish", topic: "fueling/" + deviceId + "/auth",
            payload: Buffer.from(JSON.stringify({ deviceId, vehicleId: data.vehicleId, authStatus: "success", timestamp: new Date().toISOString(), qtyAllowed: "50L" })),
            qos: 0, retain: false, dup: false });
        }
      } catch(e) {}
      
      broadcast({ type: "mqtt_message", topic, payload, deviceId, topicType, device: devices[deviceId] });
    }
    
    if (pkt.cmd === "pingresp") {}
  });
  
  parser.on("error", () => {});
  mqttSocket.on("data", d => parser.parse(d));
  
  const ping = setInterval(() => { if (mqttConnected) sendMQTT({ cmd: "pingreq" }); }, 25000);
  
  mqttSocket.on("close", () => {
    mqttConnected = false;
    clearInterval(ping);
    console.log("MQTT disconnected - reconnecting in 8s");
    broadcast({ type: "broker_disconnected" });
    setTimeout(connectMQTT, 8000);
  });
  
  mqttSocket.on("error", e => { mqttConnected = false; clearInterval(ping); });
}

const httpServer = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Content-Type", "application/json");
  const url = req.url.split("?")[0];
  
  if (url === "/health") return res.end(JSON.stringify({ ok: true, mqttConnected, deviceCount: Object.keys(devices).length, devices: Object.keys(devices), eventCount: events.length, uptime: Math.floor(process.uptime()) + "s" }));
  if (url === "/devices") return res.end(JSON.stringify(devices, null, 2));
  if (url === "/events") return res.end(JSON.stringify(events.slice(0, 50), null, 2));
  
  res.end(JSON.stringify({ ok: true, name: "FuelLink Bridge" }));
});

const wss = new WebSocket.Server({ server: httpServer });

wss.on("connection", ws => {
  wsClients.add(ws);
  ws.send(JSON.stringify({ type: "init", mqttConnected, devices, events: events.slice(0, 20) }));
  
  ws.on("message", data => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === "publish" && msg.topic) {
        const pl = typeof msg.payload === "string" ? msg.payload : JSON.stringify(msg.payload);
        sendMQTT({ cmd: "publish", topic: msg.topic, payload: Buffer.from(pl), qos: 1, retain: false, dup: false, messageId: Math.floor(Math.random() * 65535) });
      }
      if (msg.type === "get_devices") ws.send(JSON.stringify({ type: "devices", devices }));
    } catch(e) {}
  });
  
  ws.on("close", () => wsClients.delete(ws));
  ws.on("error", () => {});
});

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("FuelLink Bridge running on port " + PORT);
  connectMQTT();
});
