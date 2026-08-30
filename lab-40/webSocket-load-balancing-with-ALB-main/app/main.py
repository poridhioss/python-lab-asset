"""
Lab 40: FastAPI WebSocket Server
Demonstrates proper WebSocket upgrade headers and connection handling
behind an AWS Application Load Balancer.
"""

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import json
import asyncio
import logging
import os
from datetime import datetime
from typing import Dict, Set

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="WebSocket Load Balancing Lab", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Track active connections
active_connections: Set[WebSocket] = set()
connection_count = 0


class ConnectionManager:
    """Manages WebSocket connections and message broadcasting."""

    def __init__(self):
        self.active_connections: Dict[WebSocket, dict] = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        """Accept and register a new WebSocket connection."""
        await websocket.accept()
        self.active_connections[websocket] = {
            "client_id": client_id,
            "connected_at": datetime.now().isoformat(),
            "instance_id": os.getenv("INSTANCE_ID", "local")
        }
        logger.info(f"Client {client_id} connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        """Remove a WebSocket connection."""
        if websocket in self.active_connections:
            client_info = self.active_connections[websocket]
            del self.active_connections[websocket]
            logger.info(f"Client {client_info['client_id']} disconnected. "
                       f"Total: {len(self.active_connections)}")

    async def send_personal_message(self, message: str, websocket: WebSocket):
        """Send message to a specific client."""
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        """Broadcast message to all connected clients."""
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Error broadcasting: {e}")


manager = ConnectionManager()


@app.get("/")
async def root():
    """Root endpoint with lab information."""
    return {
        "lab": "Lab 40: WebSocket Load Balancing with ALB",
        "instance_id": os.getenv("INSTANCE_ID", "local"),
        "active_connections": len(manager.active_connections),
        "websocket_endpoint": "/ws"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for ALB target group."""
    return {
        "status": "healthy",
        "instance_id": os.getenv("INSTANCE_ID", "local"),
        "timestamp": datetime.now().isoformat()
    }


@app.get("/instance-info")
async def instance_info():
    """Return instance information to verify stickiness."""
    return {
        "instance_id": os.getenv("INSTANCE_ID", "unknown"),
        "hostname": os.uname().nodename if hasattr(os, 'uname') else "unknown",
        "active_connections": len(manager.active_connections)
    }


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Main WebSocket endpoint.
    FastAPI automatically handles the WebSocket upgrade headers:
    - Upgrade: websocket
    - Connection: Upgrade
    - Sec-WebSocket-Key
    - Sec-WebSocket-Version: 13
    """
    global connection_count
    connection_count += 1
    client_id = f"client_{connection_count}_{os.getenv('INSTANCE_ID', 'local')}"

    await manager.connect(websocket, client_id)

    # Send welcome message with instance info
    welcome_msg = {
        "type": "welcome",
        "client_id": client_id,
        "instance_id": os.getenv("INSTANCE_ID", "local"),
        "message": "Connected to WebSocket server"
    }
    await websocket.send_json(welcome_msg)

    try:
        while True:
            # Receive message from client
            data = await websocket.receive_text()

            logger.info(f"Received from {client_id}: {data}")

            # Parse message
            try:
                message_data = json.loads(data)
            except json.JSONDecodeError:
                message_data = {"type": "echo", "content": data}

            # Echo message back with instance info
            response = {
                "type": "echo",
                "instance_id": os.getenv("INSTANCE_ID", "local"),
                "client_id": client_id,
                "timestamp": datetime.now().isoformat(),
                "received": message_data
            }

            await websocket.send_json(response)

    except WebSocketDisconnect:
        manager.disconnect(websocket)
        logger.info(f"Client {client_id} disconnected normally")

    except Exception as e:
        logger.error(f"Error with client {client_id}: {e}")
        manager.disconnect(websocket)


@app.get("/ws-test")
async def websocket_test_page():
    """Simple HTML page for testing WebSocket in browser."""
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>WebSocket Test - Lab 40</title>
        <style>
            body { font-family: Arial; margin: 20px; }
            #messages { border: 1px solid #ccc; padding: 10px; height: 300px; overflow-y: scroll; }
            input { width: 70%; padding: 8px; }
            button { padding: 8px 16px; }
        </style>
    </head>
    <body>
        <h1>WebSocket Load Balancing Test</h1>
        <div id="status">Disconnected</div>
        <div id="messages"></div>
        <input id="messageInput" placeholder="Type a message..." />
        <button onclick="sendMessage()">Send</button>
        <button onclick="connect()">Connect</button>
        <button onclick="disconnect()">Disconnect</button>

        <script>
            let ws = null;

            function connect() {
                ws = new WebSocket(`ws://${window.location.host}/ws`);
                ws.onopen = () => {
                    document.getElementById('status').innerText = 'Connected';
                };
                ws.onmessage = (event) => {
                    const div = document.getElementById('messages');
                    div.innerHTML += `<p>${event.data}</p>`;
                    div.scrollTop = div.scrollHeight;
                };
                ws.onclose = () => {
                    document.getElementById('status').innerText = 'Disconnected';
                };
            }

            function sendMessage() {
                const input = document.getElementById('messageInput');
                if (ws && ws.readyState === WebSocket.OPEN) {
                    ws.send(input.value);
                    input.value = '';
                }
            }

            function disconnect() {
                if (ws) ws.close();
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info",
        # WebSocket-specific configurations
        ws_ping_interval=20,
        ws_ping_timeout=20
    )