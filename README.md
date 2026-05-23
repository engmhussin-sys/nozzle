services:
  - type: web
    name: fuellink-mqtt-broker
    runtime: node
    buildCommand: npm install
    startCommand: node fuellink-server.js
    envVars:
      - key: NODE_VERSION
        value: 20.11.0
