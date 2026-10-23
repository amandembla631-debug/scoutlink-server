
const http = require("http");
const WebSocket = require("ws");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("SCOUTLINK signaling server is running");
});

const wss = new WebSocket.Server({ server });

const rooms = new Map();

wss.on("connection", (ws) => {
  let roomCode = null;

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      if (data.type === "join") {
        if (
          typeof data.room !== "string" ||
          !/^[A-Z0-9]{6}$/.test(data.room)
        ) {
          ws.send(JSON.stringify({
            type: "error",
            message: "Room code must be 6 letters/numbers"
          }));
          return;
        }

        roomCode = data.room;
        if (!rooms.has(roomCode)) rooms.set(roomCode, new Set());

        const room = rooms.get(roomCode);

        if (room.size >= 2) {
          ws.send(JSON.stringify({
            type: "error",
            message: "Room is full"
          }));
          roomCode = null;
          return;
        }

        room.add(ws);
        ws.send(JSON.stringify({ type: "joined", room: roomCode }));

        if (room.size === 2) {
          for (const peer of room) {
            if (peer.readyState === WebSocket.OPEN) {
              peer.send(JSON.stringify({ type: "peer-ready" }));
            }
          }
        }
        return;
      }

      if (!roomCode) return;

      const room = rooms.get(roomCode);
      if (!room) return;

      for (const peer of room) {
        if (peer !== ws && peer.readyState === WebSocket.OPEN) {
          peer.send(JSON.stringify(data));
        }
      }
    } catch {
      ws.send(JSON.stringify({
        type: "error",
        message: "Invalid message"
      }));
    }
  });

  ws.on("close", () => {
    if (!roomCode) return;
    const room = rooms.get(roomCode);
    if (!room) return;

    room.delete(ws);
    for (const peer of room) {
      if (peer.readyState === WebSocket.OPEN) {
        peer.send(JSON.stringify({ type: "peer-left" }));
      }
    }
    if (room.size === 0) rooms.delete(roomCode);
  });
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, "0.0.0.0", () => {
  console.log(`SCOUTLINK server listening on ${PORT}`);
});
