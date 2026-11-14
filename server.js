const http = require('http');
const net = require('net');
const fs = require('fs');
const path = require('path');

const WEB_PORT = 3000;
const MQTT_PORT = 1885;
const PUBLIC_DIR = path.join(__dirname, 'public');

let liveTags = {
  _status: 'Waiting for gateway...',
  _lastUpdated: null
};

// 1. Built-In Zero-Dependency MQTT Broker
const mqttServer = net.createServer((socket) => {
  console.log('BL104L Gateway Connected from:', socket.remoteAddress);

  socket.on('data', (buf) => {
    let offset = 0;
    while (offset < buf.length) {
      const header = buf[offset];
      const packetType = header >> 4;
      let multiplier = 1, remLen = 0, byteIdx = offset + 1, digit;
      do {
        if (byteIdx >= buf.length) return;
        digit = buf[byteIdx++];
        remLen += (digit & 127) * multiplier;
        multiplier *= 128;
      } while ((digit & 128) !== 0);

      const packetEnd = byteIdx + remLen;
      if (packetEnd > buf.length) break;

      if (packetType === 1) {
        socket.write(Buffer.from([0x20, 0x02, 0x00, 0x00]));
      } else if (packetType === 3) {
        const qos = (header >> 1) & 0x03;
        const topicLen = buf.readUInt16BE(byteIdx);
        let payloadStart = byteIdx + 2 + topicLen;
        if (qos > 0) {
          const packetId = buf.slice(payloadStart, payloadStart + 2);
          payloadStart += 2;
          if (qos === 1) socket.write(Buffer.concat([Buffer.from([0x40, 0x02]), packetId]));
        }
        const payloadStr = buf.toString('utf8', payloadStart, packetEnd);
        try {
          const parsed = JSON.parse(payloadStr);
          if (Array.isArray(parsed.sensorDatas)) {
            parsed.sensorDatas.forEach(item => {
              const key = item.flag || item.name;
              if (key !== undefined) liveTags[key] = item.value;
            });
          } else {
            Object.assign(liveTags, parsed);
          }
          liveTags._status = 'Online';
          liveTags._lastUpdated = new Date().toLocaleTimeString();
        } catch (e) {}
      } else if (packetType === 8) {
        const packetId = buf.slice(byteIdx, byteIdx + 2);
        socket.write(Buffer.concat([Buffer.from([0x90, 0x03]), packetId, Buffer.from([0x00])]));
      } else if (packetType === 12) {
        socket.write(Buffer.from([0xD0, 0x00]));
      }
      offset = packetEnd;
    }
  });
  socket.on('error', () => {});
});

mqttServer.listen(MQTT_PORT, '0.0.0.0', () => {
  console.log('MQTT Broker listening on port ' + MQTT_PORT);
});

// 2. Serve Your HTML/CSS/Bootstrap Files + Live Tag API
const mimeTypes = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.svg': 'image/svg+xml', '.ico': 'image/x-icon'
};

http.createServer((req, res) => {
  if (req.url === '/api/tags') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify(liveTags));
  }

  let filePath = path.join(PUBLIC_DIR, req.url === '/' ? 'index.html' : req.url);
  const ext = path.extname(filePath);
  fs.readFile(filePath, (err, content) => {
    if (err) {
      res.writeHead(404);
      return res.end('File not found');
    }
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
    res.end(content);
  });
}).listen(WEB_PORT, () => {
  console.log('Web Dashboard running at http://localhost:' + WEB_PORT);
});
