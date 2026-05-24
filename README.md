# FuelLink MQTT Bridge

A Node.js service that bridges MQTT messages from the IoT broker (iot.gorex.ai:1883) to WebSocket clients, enabling real-time dashboard updates for the FuelLink fueling nozzle system.

## Features

- **MQTT Connection**: Connects to the MQTT broker and subscribes to `fueling/#` topics
- **WebSocket Server**: Real-time communication with dashboard clients
- **Device Tracking**: Automatically discovers and tracks fueling nozzle devices
- **Event Management**: Stores up to 500 recent events
- **Auth Handling**: Processes device authorization requests
- **Health Monitoring**: Built-in health check endpoint

## Environment Variables

```bash
PORT=3001                              # WebSocket/HTTP port (default: 3001)
MQTT_HOST=iot.gorex.ai                # MQTT broker host
MQTT_PORT=1883                         # MQTT broker port
MQTT_USER=fuelingnozzle                # MQTT username
MQTT_PASS=FuelingNozzle@fai            # MQTT password
```

## API Endpoints

### HTTP

- `GET /health` - Health check status
- `GET /devices` - List all connected devices
- `GET /events` - Last 50 events
- `GET /` - API info

### WebSocket (`ws://HOST:PORT`)

**Client → Server:**
```json
{ "type": "publish", "topic": "fueling/device123/command", "payload": {...}, "qos": 1 }
{ "type": "get_devices" }
```

**Server → Client:**
```json
{ "type": "init", "mqttConnected": true, "devices": {...}, "events": [...] }
{ "type": "broker_connected", "host": "iot.gorex.ai" }
{ "type": "mqtt_message", "topic": "...", "payload": "...", "deviceId": "..." }
```

## Installation

```bash
npm install
```

## Running Locally

```bash
node server.js
```

## Deployment on Railway

1. Push this repo to GitHub
2. Connect to Railway
3. Set environment variables in Railway dashboard
4. Railway will auto-deploy on push

## Directory Structure

```
.
├── server.js          # Main application
├── package.json       # Dependencies
├── railway.toml       # Railway deployment config
├── .gitignore         # Git ignore rules
└── README.md          # This file
```

## License

Proprietary - FuelLink 2026