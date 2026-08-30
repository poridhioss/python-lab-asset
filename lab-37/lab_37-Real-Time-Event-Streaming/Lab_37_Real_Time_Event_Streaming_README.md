# Lab 37 — Real-Time Event Streaming Architecture

## Introduction

Modern applications often need to show task progress in real time without refreshing the page. In this lab, you will build a real-time event streaming system where a background task publishes progress events through Redis Pub/Sub and FastAPI forwards those events to connected WebSocket clients.

## Objectives

By completing this lab, you will learn how to:

- Publish task progress using Redis Pub/Sub.
- Process background tasks using Celery.
- Build a WebSocket endpoint with FastAPI.
- Stream real-time events to browser clients using the native WebSocket API.
- Integrate a real-time frontend client.
- Build a basic real-time task progress architecture.

## 1. Architecture Overview
![Architecture](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/Architecture_1.png)

### Complete Request Flow

```text
User clicks "Start Task"
          │
          ▼
     FastAPI API
          │
          │ Create task
          ▼
      Celery Worker
          │
          │ Process task
          │
          │ Publish progress
          ▼
       Redis Pub/Sub
          │
          │ Subscribe
          ▼
      FastAPI WebSocket
          │
          │ Push event
          ▼
   Browser Frontend
          │
          ▼
   Real-Time Progress
```

## 2. Why Redis Pub/Sub?

Without real-time streaming, the frontend may repeatedly request the task status:

```text
GET /task-status
GET /task-status
GET /task-status
GET /task-status
```

This is called polling.

With Redis Pub/Sub, the worker publishes an event whenever the task progress changes:

```text
Celery Worker
     │
     │ 25%
     ▼
Redis Pub/Sub
     │
     ▼
FastAPI WebSocket
     │
     ▼
Browser
```

The browser receives progress updates immediately.

## 3. Components

| Component | Responsibility |
|---|---|
| FastAPI | HTTP API and WebSocket server |
| Celery | Executes background tasks |
| Redis | Celery broker and Pub/Sub |
| WebSocket | Pushes events to browser |
| Frontend | Displays live progress |
| Browser | Receives and displays real-time updates |

## 4. Environment Setup

### Step 1 — Update the VM

```bash
sudo apt update -y
```

Check Python:

```bash
python3 --version
```

### Step 2 — Install Redis

```bash
sudo apt install -y redis-server
```

Start Redis:

```bash
sudo systemctl start redis-server
```

Enable Redis:

```bash
sudo systemctl enable redis-server
```

Test Redis:

```bash
redis-cli ping
```

Expected:

```text
PONG
```

## 5. Create the Project

```bash
mkdir -p ~/lab37-real-time-streaming
cd ~/lab37-real-time-streaming
```

Create a virtual environment:

```bash
python3 -m venv venv
```

Activate it:

```bash
source venv/bin/activate
```

## 6. Install Dependencies

```bash
pip install "fastapi" "uvicorn[standard]" "celery" "redis>=4.2"
```

Create `requirements.txt`:

```bash
cat << 'EOF' > requirements.txt
fastapi
uvicorn[standard]
celery
redis>=4.2
EOF
```

## 7. Project Structure

```text
lab37-real-time-streaming/
│
├── app/
│   ├── __init__.py
│   ├── celery_app.py
│   ├── tasks.py
│   └── main.py
│
├── static/
│   └── index.html
│
└── requirements.txt
```

## 8. Configure Celery

Create the application directory:

```bash
mkdir -p app static
touch app/__init__.py
```

Create `app/celery_app.py`:

```bash
cat << 'EOF' > app/celery_app.py
from celery import Celery

REDIS_URL = "redis://localhost:6379/0"

celery_app = Celery(
    "lab37",
    broker=REDIS_URL,
    backend=REDIS_URL,
)

celery_app.conf.update(
    task_track_started=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)
EOF
```

## 9. Create Background Task

Create `app/tasks.py`:

```bash
cat << 'EOF' > app/tasks.py
import json
import time

import redis

from .celery_app import celery_app


redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=1,
    decode_responses=True,
)


# Note: Celery uses db=0 (broker + backend) while Pub/Sub messages
# live on db=1. Keeping them on separate logical databases avoids
# pubsub traffic interfering with the broker queue.
@celery_app.task(name="process_task")
def process_task(task_id: str):

    channel = f"task_progress:{task_id}"

    total_steps = 10

    for step in range(1, total_steps + 1):

        time.sleep(1)

        progress = int(
            (step / total_steps) * 100
        )

        event = {
            "task_id": task_id,
            "status": "processing",
            "progress": progress,
            "step": step,
            "total": total_steps,
        }

        redis_client.publish(
            channel,
            json.dumps(event)
        )

    event = {
        "task_id": task_id,
        "status": "completed",
        "progress": 100,
        "step": total_steps,
        "total": total_steps,
    }

    redis_client.publish(
        channel,
        json.dumps(event)
    )

    return event
EOF
```

## 10. Create FastAPI WebSocket Server

Create `app/main.py`:

```bash
cat << 'EOF' > app/main.py
import asyncio
import json
import uuid

import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from .tasks import process_task


app = FastAPI(
    title="Lab 37 - Real-Time Event Streaming"
)


@app.get("/")
async def home():
    return FileResponse("static/index.html")


@app.post("/tasks")
async def create_task():

    task_id = str(uuid.uuid4())

    # Give the client a moment to open the WebSocket
    # before the worker publishes its first event.
    await asyncio.sleep(0.5)

    process_task.delay(task_id)

    return {
        "task_id": task_id,
        "status": "queued",
        "websocket": f"/ws/{task_id}",
    }


@app.websocket("/ws/{task_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    task_id: str,
):

    await websocket.accept()

    redis_client = redis.Redis(
        host="localhost",
        port=6379,
        db=1,
        decode_responses=True,
    )

    pubsub = redis_client.pubsub()

    channel = f"task_progress:{task_id}"

    await pubsub.subscribe(channel)

    try:

        while True:

            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message:

                data = json.loads(
                    message["data"]
                )

                await websocket.send_json(data)

                if data["status"] == "completed":
                    break

            await asyncio.sleep(0.05)

    except WebSocketDisconnect:
        pass

    finally:

        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()
EOF
```

## 11. Create Frontend

Create `static/index.html`:

```bash
cat << 'EOF' > static/index.html
<!DOCTYPE html>

<html lang="en">

<head>

<meta charset="UTF-8">

<title>Real-Time Task Progress</title>

<style>

body {
    font-family: Arial, sans-serif;
    max-width: 700px;
    margin: 50px auto;
    padding: 20px;
}

button {
    padding: 10px 20px;
    font-size: 16px;
    cursor: pointer;
}

.progress-container {
    width: 100%;
    height: 30px;
    background: #ddd;
    margin-top: 25px;
}

.progress-bar {
    height: 100%;
    width: 0%;
    background: #4caf50;
    color: white;
    text-align: center;
    line-height: 30px;
}

#status {
    margin-top: 20px;
    padding: 15px;
    background: #f4f4f4;
    white-space: pre-line;
}

</style>

</head>

<body>

<h1>Real-Time Task Progress</h1>

<button onclick="startTask()">
Start Task
</button>

<div class="progress-container">

<div
    id="progress"
    class="progress-bar"
>
0%
</div>

</div>

<div id="status">
Waiting for task...
</div>

<script>

async function startTask() {

    const status =
        document.getElementById("status");

    const progress =
        document.getElementById("progress");

    status.textContent =
        "Creating task...";

    const response =
        await fetch(
            "/tasks",
            {
                method: "POST"
            }
        );

    const task =
        await response.json();

    status.textContent =
        `Task ID: ${task.task_id}
Status: ${task.status}`;

    const protocol =
        window.location.protocol === "https:"
        ? "wss"
        : "ws";

    const socket =
        new WebSocket(
            `${protocol}://${window.location.host}/ws/${task.task_id}`
        );

    socket.onmessage =
        function(event) {

            const data =
                JSON.parse(event.data);

            progress.style.width =
                `${data.progress}%`;

            progress.textContent =
                `${data.progress}%`;

            status.textContent =
                `Task ID: ${data.task_id}
Status: ${data.status}
Step: ${data.step}/${data.total}
Progress: ${data.progress}%`;

            if (
                data.status === "completed"
            ) {

                socket.close();

            }

        };

    socket.onerror =
        function() {

            status.textContent +=
                "\nWebSocket connection error.";

        };

}

</script>

</body>

</html>
EOF
```

## 12. Start Celery Worker

Open **Terminal 1**:

```bash
cd ~/lab37-real-time-streaming
source venv/bin/activate
```

Start the worker:

```bash
celery -A app.celery_app worker --loglevel=info
```

Expected output should contain:

```text
[tasks]
  . process_task
```

and:

```text
celery@... ready.
```

## 13. Start FastAPI

Open **Terminal 2**:

```bash
cd ~/lab37-real-time-streaming
source venv/bin/activate
```

Run:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Expected:

```text
Uvicorn running on http://0.0.0.0:8000
```

## 14. Test the API

Open **Terminal 3**.

Run:

```bash
curl -X POST http://127.0.0.1:8000/tasks
```

Expected:

```json
{
    "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "status": "queued",
    "websocket": "/ws/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

## 15. Test Real-Time UI

Open the Poridhi VM's exposed port `8000`.

For example:

```text
http://<VM-IP>:8000
```

or use the URL provided by the Poridhi VM environment.

You should see:

```text
Real-Time Task Progress

[ Start Task ]

0%
```

Click **Start Task**.

The progress bar should fill automatically, advancing one step per second:

```text
0% → 10% → 20% → 30% → 40% → 50% → 60% → 70% → 80% → 90% → 100%
```

The page does not need to be refreshed — the browser receives each update live via the WebSocket connection.

## 16. Verify Redis Pub/Sub

Open another terminal:

```bash
redis-cli
```

Subscribe to the task channel:

```text
SUBSCRIBE task_progress:<TASK_ID>
```

When the worker processes the task, you should receive events similar to:

```json
{
    "task_id": "...",
    "status": "processing",
    "progress": 30,
    "step": 3,
    "total": 10
}
```

Final event:

```json
{
    "task_id": "...",
    "status": "completed",
    "progress": 100,
    "step": 10,
    "total": 10
}
```

## 17. Verify Celery Worker

The Celery terminal should show:

```text
Task process_task[...] received
```

and after completion:

```text
Task process_task[...] succeeded
```

The complete event flow is:

```text
Celery Worker
      │
      │ publish()
      ▼
Redis Pub/Sub
      │
      │ subscribe()
      ▼
FastAPI WebSocket
      │
      │ send_json()
      ▼
Browser
```

## 18. Real-Time Frontend Architecture

The lab implementation uses the **browser's native WebSocket API** to connect directly to the FastAPI WebSocket endpoint. This is the simplest path and works well for learning.

### Native WebSocket (implemented in this lab)

```text
┌──────────────────────┐
│   Browser WebSocket  │
│       Frontend       │
└──────────┬───────────┘
           │
           │ ws:// or wss://
           ▼
┌──────────────────────┐
│       FastAPI        │
│   WebSocket Server   │
└──────────┬───────────┘
           │
           │ Subscribe
           ▼
┌──────────────────────┐
│        Redis         │
│      Pub / Sub       │
└──────────▲───────────┘
           │
           │ Publish
           │
┌──────────┴───────────┐
│    Celery Worker     │
│   Background Task    │
└──────────────────────┘
```

### Socket.IO (production consideration)

For production-grade frontends, **Socket.IO** is a common choice because it adds features that raw WebSockets don't provide out of the box:

- Automatic reconnection on dropped connections
- Rooms and namespaces for multi-tenant streaming
- HTTP long-polling fallback for restrictive networks
- Built-in acknowledgement / event semantics

> **Important:** Socket.IO and plain WebSocket are **different protocols**. The browser Socket.IO client cannot connect directly to a FastAPI `@app.websocket(...)` endpoint. To use Socket.IO on the frontend, the backend must run a Socket.IO-compatible server (e.g., `python-socketio` mounted as an ASGI app), and the data path then becomes:

```text
Socket.IO Client
      │
      ▼
Socket.IO Server (python-socketio / ASGI)
      │
      ▼
Redis Pub / Sub
      │
      ▼
Celery Worker
```

The core architecture — **Celery → Redis Pub/Sub → server → browser** — stays the same; only the wire protocol between browser and server changes.

## 19. Troubleshooting

### Redis connection error

```bash
redis-cli ping
```

Expected:

```text
PONG
```

If Redis is not running:

```bash
sudo systemctl restart redis-server
```

### Celery Worker does not start

Run from the project root:

```bash
cd ~/lab37-real-time-streaming
source venv/bin/activate
celery -A app.celery_app worker --loglevel=info
```

Make sure `process_task` appears under `[tasks]`.

### FastAPI is not accessible

Start FastAPI using:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Make sure port `8000` is exposed by the Poridhi VM.

### WebSocket connection error

Open the application using the same FastAPI host and port:

```text
http://<VM-IP>:8000
```

If the WS fails, ensure you're accessing the page through the same exposed URL — not `127.0.0.1` from outside the VM, which would only work for port-forwarded SSH tunnels.

The frontend automatically selects:

```text
ws://
```

for HTTP and:

```text
wss://
```

for HTTPS.

> **Tip:** If pubsub messages aren't reaching the worker, double-check both `redis_client` instances (in `tasks.py` and `main.py`) use `db=1` while Celery's broker uses `db=0`.

## 20. Expected Output

### Celery Worker

```text
[tasks]
  . process_task

celery@... ready.
```

### FastAPI

```text
Uvicorn running on http://0.0.0.0:8000
```

### Task Creation

```json
{
    "task_id": "...",
    "status": "queued",
    "websocket": "/ws/..."
}
```

### Real-Time Progress

```text
10%
20%
30%
40%
50%
60%
70%
80%
90%
100%
```

### Final Event

```json
{
    "task_id": "...",
    "status": "completed",
    "progress": 100,
    "step": 10,
    "total": 10
}
```

## 21. Final Architecture

```text
                       USER
                        │
                        ▼
               ┌────────────────┐
               │   Web UI        │
               │ WebSocket Client│
               └───────┬────────┘
                       │
                       │ Real-Time
                       ▼
               ┌────────────────┐
               │    FastAPI     │
               │ HTTP + WebSocket│
               └───────┬────────┘
                       │
                       │ Subscribe
                       ▼
               ┌────────────────┐
               │     Redis      │
               │   Pub / Sub    │
               └───────▲────────┘
                       │
                       │ Publish
                       │
               ┌───────┴────────┐
               │ Celery Worker  │
               │ Background Task│
               └────────────────┘
```

## Conclusion

In this lab, you built a real-time event streaming system using **Celery, Redis Pub/Sub, FastAPI WebSocket, and a browser-based native WebSocket client**. The system delivers background task progress to users instantly without polling or page refreshes.

The same architecture can be extended to production by swapping the native WebSocket layer for Socket.IO on both the frontend and backend, while keeping Celery and Redis Pub/Sub unchanged.
