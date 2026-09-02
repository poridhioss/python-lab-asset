#!/bin/bash
# User data script for EC2 instances - Lab 40
# Installs Docker, builds the WebSocket application, and runs it

set -e

# Update system
yum update -y

# Install Docker
yum install -y docker
systemctl start docker
systemctl enable docker

# Add ec2-user to docker group
usermod -a -G docker ec2-user

# Install Python and pip (for health checks)
yum install -y python3

# Create application directory
mkdir -p /opt/websocket-app
cd /opt/websocket-app

# Create the FastAPI application
cat > main.py <<'PYTHON_EOF'
$(cat /tmp/app_main.py 2>/dev/null || echo "")
PYTHON_EOF

# Create requirements.txt
cat > requirements.txt <<'REQUIREMENTS_EOF'
fastapi==0.104.1
uvicorn[standard]==0.24.0
websockets==12.0
pydantic==2.5.0
REQUIREMENTS_EOF

# Create Dockerfile
cat > Dockerfile <<'DOCKERFILE_EOF'
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
DOCKERFILE_EOF

# Note: In production, you would copy the actual app files
# For this lab, we'll install Python packages directly and run with uvicorn

# Install Python packages
pip3 install --no-cache-dir fastapi==0.104.1 uvicorn[standard]==0.24.0 websockets==12.0 pydantic==2.5.0

# Create the application file from local source
cat > /opt/websocket-app/main.py <<'APP_EOF'
"""
Lab 40: FastAPI WebSocket Server
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import os
import socket
from datetime import datetime

app = FastAPI(title="WebSocket Load Balancing Lab", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

INSTANCE_ID = "${instance_id}-$(hostname -s)"
connection_count = 0

@app.get("/")
async def root():
    return {
        "lab": "Lab 40: WebSocket Load Balancing",
        "instance_id": INSTANCE_ID,
        "hostname": socket.gethostname()
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "instance_id": INSTANCE_ID,
        "hostname": socket.gethostname()
    }

@app.get("/instance-info")
async def instance_info():
    return {
        "instance_id": INSTANCE_ID,
        "hostname": socket.gethostname(),
        "ip": socket.gethostbyname(socket.gethostname())
    }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global connection_count
    connection_count += 1
    client_id = f"client_{connection_count}"

    await websocket.accept()

    await websocket.send_json({
        "type": "welcome",
        "client_id": client_id,
        "instance_id": INSTANCE_ID,
        "hostname": socket.gethostname(),
        "message": "Connected to WebSocket server"
    })

    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
            except:
                msg = {"content": data}

            await websocket.send_json({
                "type": "echo",
                "instance_id": INSTANCE_ID,
                "hostname": socket.gethostname(),
                "client_id": client_id,
                "received": msg,
                "timestamp": datetime.now().isoformat()
            })
    except WebSocketDisconnect:
        print(f"Client {client_id} disconnected")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
APP_EOF

# Start the application as a systemd service
cat > /etc/systemd/system/websocket-app.service <<'SERVICE_EOF'
[Unit]
Description=WebSocket Application
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/websocket-app
ExecStart=/usr/local/bin/uvicorn main:app --host 0.0.0.0 --port 8000
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
SERVICE_EOF

# Reload systemd and start service
systemctl daemon-reload
systemctl enable websocket-app
systemctl start websocket-app

# Wait for app to start
sleep 10

# Verify the app is running
systemctl status websocket-app --no-pager

echo "WebSocket application deployed successfully on $(hostname)"