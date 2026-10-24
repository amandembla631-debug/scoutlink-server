
const http = require("http");
const WebSocket = require("ws");

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("SCOUTLINK signaling server is running");
});

const wss = new WebSocket.Server({
  server,
  maxPayload: 64 * 1024
});

const rooms = new Map();

function sendJson(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function removeFromRoom(ws) {
  const roomCode = ws.roomCode;
  if (!roomCode) return;

  const room = rooms.get(roomCode);
  ws.roomCode = null;

  if (!room) return;

  room.delete(ws);

  for (const peer of room) {
    sendJson(peer, { type: "peer-left" });
  }

  if (room.size === 0) {
    rooms.delete(roomCode);
  }
}

wss.on("connection", (ws) => {
  ws.roomCode = null;

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message.toString());

      if (!data || typeof data.type !== "string") {
        sendJson(ws, {
          type: "error",
          message: "Invalid message format"
        });
        return;
      }

      if (data.type === "join") {
        if (ws.roomCode) {
          sendJson(ws, {
            type: "error",
            message: "Already joined a room"
          });
          return;
        }

        if (
          typeof data.room !== "string" ||
          !/^[A-Z0-9]{6}$/.test(data.room) ||
          !["transmitter", "receiver"].includes(data.role)
        ) {
          sendJson(ws, {
            type: "error",
            message: "Invalid room code or role"
          });
          return;
        }

        const roomCode = data.room;

        if (!rooms.has(roomCode)) {
          rooms.set(roomCode, new Map());
        }

        const room = rooms.get(roomCode);

        if (room.size >= 2) {
          sendJson(ws, {
            type: "error",
            message: "Room is full"
          });
          return;
        }

        for (const client of room.values()) {
          if (client.role === data.role) {
            sendJson(ws, {
              type: "error",
              message: "A device with this role is already in the room"
            });
            return;
          }
        }

        ws.roomCode = roomCode;
        ws.role = data.role;
        room.set(ws, { role: data.role });

        sendJson(ws, { type: "joined", room: roomCode });

        if (room.size === 2) {
          for (const peer of room.keys()) {
            sendJson(peer, { type: "peer-ready" });
          }
        }

        return;
      }

      if (!ws.roomCode) {
        sendJson(ws, {
          type: "error",
          message: "Join a room first"
        });
        return;
      }

      const room = rooms.get(ws.roomCode);
      if (!room) return;

      const allowedTypes = ["offer", "answer", "ice", "sensor"];

      if (!allowedTypes.includes(data.type)) {
        sendJson(ws, {
          type: "error",
          message: "Unsupported message type"
        });
        return;
      }

      for (const peer of room.keys()) {
        if (peer !== ws) {
          sendJson(peer, data);
        }
      }
    } catch {
      sendJson(ws, {
        type: "error",
        message: "Invalid message"
      });
    }
  });

  ws.on("close", () => removeFromRoom(ws));
  ws.on("error", () => removeFromRoom(ws));
});

const PORT = process.env.PORT || 10000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`SCOUTLINK server listening on ${PORT}`);
});                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
