[fuellink-server.js](https://github.com/user-attachments/files/28166715/fuellink-server.js)
/**
 * FuelLink — MQTT Broker + WebSocket + HTTP API
 * ذاتي الاحتواء — يعمل بدون أي سيرفر خارجي
 */
const net        = require('net');
const http       = require('http');
const mqttPacket = require('./node_modules/mqtt-packet');
const WS         = require('./node_modules/ws');

const MQTT_PORT = 5555;
const WS_PORT   = 8080;
const MQTT_USER = 'fuelingnozzle';
const MQTT_PASS = 'FuelingNozzle@fai';

const devices   = {};
const events    = [];
const wsClients = new Set();
const sockets   = {}; // deviceId → socket (for sending commands)

// ═══════════════════════════════════════════
//  MQTT BROKER (raw TCP)
// ═══════════════════════════════════════════
function broadcast(data) {
  const s = JSON.stringify(data);
  wsClients.forEach(c => c.readyState === 1 && c.send(s));
}

function sendToDevice(deviceId, pkt) {
  const sock = sockets[deviceId];
  if (sock && !sock.destroyed) {
    try { sock.write(mqttPacket.generate(pkt)); return true; }
    catch(e) { return false; }
  }
  return false;
}

function handleMQTTClient(socket) {
  const parser = mqttPacket.parser({ protocolVersion: 4 });
  let clientId = '', deviceId = '';

  parser.on('packet', pkt => {

    if (pkt.cmd === 'connect') {
      clientId = pkt.clientId;
      deviceId = clientId;
      const ok = pkt.username === MQTT_USER && pkt.password?.toString() === MQTT_PASS;
      console.log(`[MQTT] CONNECT ${clientId} → ${ok ? '✓ OK' : '✗ REJECTED'}`);
      socket.write(mqttPacket.generate({ cmd:'connack', sessionPresent:false, returnCode: ok ? 0 : 4 }));
      if (!ok) { socket.destroy(); return; }
      sockets[deviceId] = socket;
      if (!devices[deviceId]) devices[deviceId] = { deviceId, msgCount:0, topics:[], firstSeen:new Date().toISOString() };
      broadcast({ type:'device_connect', deviceId, ts:new Date().toISOString() });
    }

    if (pkt.cmd === 'publish') {
      const topic   = pkt.topic;
      const payload = pkt.payload.toString();
      if (pkt.qos === 1) socket.write(mqttPacket.generate({ cmd:'puback', messageId:pkt.messageId }));
      console.log(`[↓] ${topic} → ${payload.substring(0,100)}`);

      const parts = topic.split('/');
      if (parts[0] === 'fueling' && parts.length >= 3) {
        const did = parts[1], tt = parts[2];
        if (!devices[did]) devices[did] = { deviceId:did, msgCount:0, topics:[], firstSeen:new Date().toISOString() };
        const dev = devices[did];
        dev.msgCount++; dev.lastSeen = new Date().toISOString();
        if (!dev.topics.includes(tt)) dev.topics.push(tt);

        try {
          const d = JSON.parse(payload);
          if (tt === 'status') Object.assign(dev, { status:d.status, battery:d.battery, firmware:d.firmware, hardware:d.hardware, name:d.name });
          if (tt === 'event')  {
            const evt = { ...d, deviceId:did, receivedAt:new Date().toISOString() };
            events.unshift(evt);
            if (events.length > 500) events.length = 500;
            console.log(`[Event] ${did}: ${d.fuelQuantityLiters}L ${d.fuelType} = ${d.totalCost} SAR`);
          }
          if (tt === 'auth') {
            const resp = JSON.stringify({ deviceId:did, vehicleId:d.vehicleId, authStatus:'success', timestamp:new Date().toISOString(), qtyAllowed:'50L', costAllowed:'500SAR' });
            socket.write(mqttPacket.generate({ cmd:'publish', topic:`fueling/${did}/auth`, payload:resp, qos:0, retain:false, dup:false }));
            console.log(`[Auth] ✓ success → ${did} vehicleId:${d.vehicleId}`);
          }
        } catch(e) {}

        broadcast({ type:'mqtt_message', topic, payload, deviceId:did, topicType:tt, device:devices[did] });
      }
    }

    if (pkt.cmd === 'subscribe') {
      const granted = pkt.subscriptions.map(s => s.qos);
      socket.write(mqttPacket.generate({ cmd:'suback', messageId:pkt.messageId, granted }));
    }

    if (pkt.cmd === 'pingreq') socket.write(mqttPacket.generate({ cmd:'pingresp' }));
    if (pkt.cmd === 'disconnect') { socket.destroy(); }
  });

  parser.on('error', e => {});
  socket.on('data', d => parser.parse(d));
  socket.on('close', () => {
    if (deviceId) {
      delete sockets[deviceId];
      if (devices[deviceId]) devices[deviceId].status = 'disconnected';
      console.log(`[MQTT] ✗ قطع: ${deviceId}`);
      broadcast({ type:'device_disconnect', deviceId, ts:new Date().toISOString() });
    }
  });
  socket.on('error', ()=>{});
}

// ═══════════════════════════════════════════
//  HTTP + WebSocket SERVER
// ═══════════════════════════════════════════
const httpSrv = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Content-Type', 'application/json');
  const url = req.url.split('?')[0];

  if (url === '/health')  return res.end(JSON.stringify({ ok:true, broker:`port ${MQTT_PORT}`, ws:`port ${WS_PORT}`, deviceCount:Object.keys(devices).length, devices:Object.keys(devices), eventCount:events.length, uptime:Math.floor(process.uptime())+'s' }, null, 2));
  if (url === '/devices') return res.end(JSON.stringify(devices, null, 2));
  if (url === '/events')  return res.end(JSON.stringify(events.slice(0,50), null, 2));
  res.end(JSON.stringify({ ok:true, info:'FuelLink MQTT Broker', endpoints:['/health','/devices','/events'] }));
});

const wss = new WS.WebSocketServer({ server:httpSrv });
wss.on('connection', ws => {
  wsClients.add(ws);
  ws.send(JSON.stringify({ type:'init', devices, events:events.slice(0,20), mqtt_port:MQTT_PORT }));
  ws.on('message', data => {
    try {
      const msg = JSON.parse(data.toString());
      if (msg.type === 'publish' && msg.topic && msg.deviceId) {
        const pl = typeof msg.payload === 'string' ? msg.payload : JSON.stringify(msg.payload);
        const sent = sendToDevice(msg.deviceId, { cmd:'publish', topic:msg.topic, payload:pl, qos:0, retain:false, dup:false });
        console.log(`[↑] ${msg.topic} → ${sent?'sent':'device offline'}`);
      }
      if (msg.type === 'get_devices') ws.send(JSON.stringify({ type:'devices', devices }));
    } catch(e) {}
  });
  ws.on('close', () => wsClients.delete(ws));
});

// ═══════════════════════════════════════════
//  START BOTH SERVERS
// ═══════════════════════════════════════════
const mqttServer = net.createServer(handleMQTTClient);
mqttServer.listen(MQTT_PORT, '0.0.0.0', () => {
  console.log(`\n╔═══════════════════════════════════════════╗`);
  console.log(`║   FuelLink MQTT Broker — يعمل الآن       ║`);
  console.log(`╠═══════════════════════════════════════════╣`);
  console.log(`║   MQTT TCP  : port ${MQTT_PORT} (للفوهات)         ║`);
  console.log(`║   WebSocket : port ${WS_PORT} (للواجهة)        ║`);
  console.log(`║   HTTP API  : port ${WS_PORT}/health           ║`);
  console.log(`╠═══════════════════════════════════════════╣`);
  console.log(`║   الفوهة تتصل على:                       ║`);
  console.log(`║   mqtt://THIS_SERVER_IP:${MQTT_PORT}            ║`);
  console.log(`╚═══════════════════════════════════════════╝\n`);
  console.log('⏳ ينتظر اتصال الفوهات...\n');
});

httpSrv.listen(WS_PORT, '0.0.0.0', () => {
  console.log(`[WS] WebSocket + HTTP API على port ${WS_PORT}`);
});

setInterval(() => {
  const n = Object.keys(devices).length;
  if (n > 0) console.log(`[Status] ${n} جهاز | ${events.length} حدث | uptime:${Math.floor(process.uptime())}s`);
}, 30000);

process.on('SIGINT', () => { mqttServer.close(); httpSrv.close(); process.exit(0); });
