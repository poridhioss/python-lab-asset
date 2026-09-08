# Lab 37 — Real-Time Event Streaming Architecture

## Introduction

Modern applications often need to show task progress in real time without refreshing the page. In this lab, you will build a real-time event streaming system where a background task publishes progress events through Redis Pub/Sub and FastAPI forwards those events to connected WebSocket clients. You will then extend the architecture with a production-grade **Socket.IO** integration.

## Objectives

By completing this lab, you will learn how to:

- Publish task progress using Redis Pub/Sub.
- Process background tasks using Celery.
- Build a WebSocket endpoint with FastAPI.
- Stream real-time events to browser clients using the native WebSocket API.
- Integrate a real-time frontend client.
- Architect real-time UI integration with a Socket.IO frontend.
- Build a basic real-time task progress architecture.

## 1. Architecture 

![Architecture](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/Architecture_1.png)

### Complete Request Flow

```text
1. Browser  → POST /tasks              → FastAPI queues a Celery task
2. Celery   → process_task(task_id)    → runs in the background worker
3. Celery   → redis.publish(channel)   → pushes a progress event per step
4. FastAPI  → /ws/{task_id}            → subscribed to the same Redis channel
5. FastAPI  → websocket.send_json()    → forwards the event to the browser
6. Browser  → socket.onmessage         → updates the progress bar live
```

Steps 1–2 happen once per task. Steps 3–5 repeat for every progress event until a `completed` (or `failed`) event closes the loop.

## 2. Why Redis Pub/Sub?

Without real-time streaming, the frontend may repeatedly request the task status:

```text
GET /task-status
GET /task-status
GET /task-status
GET /task-status
```

This is called polling. It wastes bandwidth, adds latency, and does not scale well.

With Redis Pub/Sub, the worker publishes an event whenever the task progress changes:

```text
Celery Worker
     │
     │ 10%
     │ 20%
     │ 30%
     │  ...
     │ 100%
     ▼
Redis Pub/Sub
     │
     ▼
FastAPI WebSocket
     │
     ▼
Browser
```

![Lab 37 Output](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageouptput.png)

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

![Lab 37 Output](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageouptput.png)

Check Python:

```bash
python3 --version
```

### Step 2 — Install Redis

```bash
sudo apt install -y redis-server
```

![Lab 37 Output 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput2.png)

Start Redis:

```bash
sudo systemctl start redis-server
```

![Lab 37 Output 3](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput3.png)

Enable Redis:

```bash
sudo systemctl enable redis-server
```

![Lab 37 Output 4](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput4.png)

Test Redis:

```bash
redis-cli ping
```

Expected:

```text
PONG
```

![Lab 37 Output 5](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput5.png)

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
pip install "fastapi" "uvicorn[standard]" "celery" "redis>=4.2" "python-socketio[asyncio_client]"
```

Create `requirements.txt`:

```bash
cat << 'EOF' > requirements.txt
fastapi
uvicorn[standard]
celery
redis>=4.2
python-socketio[asyncio_client]
EOF
```

> **Note:** `python-socketio` is installed up front because Section 19 adds a Socket.IO frontend on top of the same backend — no separate install step needed later.

## 7. Project Structure

```text
lab37-real-time-streaming/
│
├── app/
│   ├── __init__.py
│   ├── celery_app.py
│   ├── tasks.py
│   ├── main.py
│   ├── socketio_app.py
│   └── socketio_events.py
│
├── static/
│   ├── index.html
│   └── socketio_index.html
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


# Celery uses db=0 (broker + backend) while Pub/Sub messages
# live on db=1. Keeping them on separate logical databases avoids
# pubsub traffic interfering with the broker queue.
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=1,
    decode_responses=True,
)


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

    # Give the client a moment to open the WebSocket / join the
    # Socket.IO room before the worker publishes its first event.
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


@app.get("/socketio")
async def socketio_home():
    return FileResponse("static/socketio_index.html")
EOF
```

## 11. Create Frontend (Native WebSocket)

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

<button onclick="startTask()">Start Task</button>

<div class="progress-container">
    <div id="progress" class="progress-bar">0%</div>
</div>

<div id="status">Waiting for task...</div>

<script>

async function startTask() {

    const status = document.getElementById("status");
    const progress = document.getElementById("progress");

    status.textContent = "Creating task...";

    const response = await fetch("/tasks", { method: "POST" });
    const task = await response.json();

    status.textContent =
        `Task ID: ${task.task_id}\nStatus: ${task.status}`;

    const protocol =
        window.location.protocol === "https:" ? "wss" : "ws";

    const socket = new WebSocket(
        `${protocol}://${window.location.host}/ws/${task.task_id}`
    );

    socket.onmessage = function (event) {

        const data = JSON.parse(event.data);

        progress.style.width = `${data.progress}%`;
        progress.textContent = `${data.progress}%`;

        status.textContent =
            `Task ID: ${data.task_id}\n` +
            `Status: ${data.status}\n` +
            `Step: ${data.step}/${data.total}\n` +
            `Progress: ${data.progress}%`;

        if (data.status === "completed") {
            socket.close();
        }
    };

    socket.onerror = function () {
        status.textContent += "\nWebSocket connection error.";
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

![Lab 37 Output 6](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput6.png)

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

![Lab 37 Output 8](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput8.png)

Expected:

```text
Uvicorn running on http://0.0.0.0:8000
```

![Lab 37 Output 10](https://raw.githubusercontent.com/poridhioss/python-lab-asset/95ebbaf0b0e59185c2851b5f4ad1824a953edf5e/lab37imageoutput10.png)

> **Note:** This command is only used up through Section 18. From Section 19 onward, once Socket.IO is wired in, the run command changes to `uvicorn app.main:socket_app ...`.

## 14. Test the API

Open **Terminal 3**.

Run:

```bash
curl -X POST http://127.0.0.1:8000/tasks
```

![Lab 37 Output 11](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput11.png)

Expected:

![Lab 37 Output 12](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput12.png)

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

![Lab 37 Output 13](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab37imageoutput13.png)

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

The complete event flow for one task looks like this end to end:

```text
Browser              FastAPI                Redis (db=1)          Celery Worker
  │  POST /tasks        │                         │                     │
  ├─────────────────────►                         │                     │
  │                      ├── process_task.delay()  ├─────────────────────►
  │  open WS /ws/<id>    │                         │                     │
  ├─────────────────────►│                         │                     │
  │                      ├── SUBSCRIBE ────────────►                     │
  │                      │                         │◄──── PUBLISH 10% ───┤
  │◄── {progress:10} ────┤◄────────────────────────┤                     │
  │        ...           │           ...           │        ...         │
  │◄── {status:completed}┤◄────────────────────────┤◄── PUBLISH 100% ────┤
  │  socket.close()      │                         │                     │
```

## 18. Real-Time Frontend Approaches

The lab implementation so far uses the **browser's native WebSocket API** to connect directly to the FastAPI WebSocket endpoint. This is the simplest path and works well for learning.

### Socket.IO (production consideration)

For production-grade frontends, **Socket.IO** is a common choice because it adds features that raw WebSockets don't provide out of the box:

- Automatic reconnection on dropped connections
- Rooms and namespaces for multi-tenant streaming
- HTTP long-polling fallback for restrictive networks
- Built-in acknowledgement / event semantics

Section 19 below actually builds this integration on top of the same Celery → Redis Pub/Sub backend, rather than just describing it.

## 19. Socket.IO Integration (Production Real-Time UI)

This section architects and builds a Socket.IO frontend on top of the existing pipeline, replacing the raw WebSocket client with a Socket.IO client and a `python-socketio` server mounted on FastAPI.

### 19.1 Why change anything?

The Redis Pub/Sub → FastAPI part of the architecture does **not** change. Only the last hop — how the browser receives events — changes:

```text
Celery Worker → Redis Pub/Sub → python-socketio server → Socket.IO client (browser)
```

### 19.2 Create the Socket.IO server

Create `app/socketio_app.py`:

```bash
cat << 'EOF' > app/socketio_app.py
import socketio

# async_mode="asgi" lets this server run inside the same process as FastAPI.
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
)
EOF
```

### 19.3 Bridge Redis Pub/Sub into Socket.IO rooms

Each task gets its own **room** (named after `task_id`). When a browser subscribes to a task, the server starts (if not already running) a background listener that reads the same `task_progress:<task_id>` Redis channel used by the raw WebSocket version, and re-emits every event into that room.

Create `app/socketio_events.py`:

```bash
cat << 'EOF' > app/socketio_events.py
import asyncio
import json

import redis.asyncio as redis

from .socketio_app import sio

# Tracks one Redis-listener task per task_id so we don't
# start duplicate subscriptions if multiple clients join
# the same room.
_active_listeners: dict[str, asyncio.Task] = {}


async def _listen_and_forward(task_id: str) -> None:
    """Subscribe to task_progress:<task_id> on Redis (db=1) and
    re-emit every event to the matching Socket.IO room."""

    channel = f"task_progress:{task_id}"

    redis_client = redis.Redis(
        host="localhost",
        port=6379,
        db=1,
        decode_responses=True,
    )
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)

    try:
        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message:
                data = json.loads(message["data"])

                # "progress" is the Socket.IO event name the
                # frontend listens for.
                await sio.emit("progress", data, room=task_id)

                if data.get("status") in ("completed", "failed"):
                    break

            await asyncio.sleep(0.05)

    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()
        _active_listeners.pop(task_id, None)


@sio.event
async def connect(sid, environ):
    # A client connected but has not joined a task room yet.
    pass


@sio.event
async def subscribe(sid, data):
    """Client calls socket.emit('subscribe', {task_id}) after
    creating a task, to join that task's room."""

    task_id = data.get("task_id") if data else None
    if not task_id:
        return

    await sio.enter_room(sid, task_id)

    if task_id not in _active_listeners:
        _active_listeners[task_id] = asyncio.create_task(
            _listen_and_forward(task_id)
        )


@sio.event
async def disconnect(sid):
    pass
EOF
```

### 19.4 Mount Socket.IO on the FastAPI app

Every existing route (`/`, `POST /tasks`, the raw `/ws/{task_id}` WebSocket, `/socketio`) stays exactly as it is in `app/main.py` — Socket.IO is added as a wrapper ASGI app around it:

```bash
cat << 'EOF' >> app/main.py

# --- Socket.IO integration -------------------------------------------------
# python-socketio wraps the existing FastAPI app as a fallback: any request
# that isn't a socket.io handshake/request is forwarded straight to `app`
# unchanged, so every route above keeps working exactly as before.

from .socketio_app import sio  # noqa: E402
from . import socketio_events  # noqa: E402,F401  (registers the event handlers)
import socketio as _socketio  # noqa: E402

socket_app = _socketio.ASGIApp(sio, other_asgi_app=app)
EOF
```

From now on, run the server against `socket_app` instead of `app`:

```bash
uvicorn app.main:socket_app --host 0.0.0.0 --port 8000
```

> **Note:** `app.main:app` would only serve the plain FastAPI routes. `app.main:socket_app` serves both the FastAPI routes *and* the Socket.IO endpoint on the same port.

### 19.5 Socket.IO frontend client

Create `static/socketio_index.html`:

```bash
cat << 'EOF' > static/socketio_index.html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Real-Time Task Progress (Socket.IO)</title>
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
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

<h1>Real-Time Task Progress (Socket.IO)</h1>

<button onclick="startTask()">Start Task</button>

<div class="progress-container">
    <div id="progress" class="progress-bar">0%</div>
</div>

<div id="status">Waiting for task...</div>

<script>
// Connects to the same origin. Socket.IO handles the handshake,
// reconnection, and transport fallback automatically.
const socket = io();

socket.on("connect_error", () => {
    document.getElementById("status").textContent +=
        "\nSocket.IO connection error.";
});

async function startTask() {
    const status = document.getElementById("status");
    const progress = document.getElementById("progress");

    status.textContent = "Creating task...";

    const response = await fetch("/tasks", { method: "POST" });
    const task = await response.json();

    status.textContent =
        `Task ID: ${task.task_id}\nStatus: ${task.status}`;

    // Join the room for this task_id so we only receive
    // events meant for this task.
    socket.emit("subscribe", { task_id: task.task_id });

    socket.on("progress", function (data) {
        if (data.task_id !== task.task_id) return; // ignore other tasks

        progress.style.width = `${data.progress}%`;
        progress.textContent = `${data.progress}%`;

        status.textContent =
            `Task ID: ${data.task_id}\n` +
            `Status: ${data.status}\n` +
            `Step: ${data.step}/${data.total}\n` +
            `Progress: ${data.progress}%`;

        if (data.status === "completed" || data.status === "failed") {
            socket.off("progress");
        }
    });
}
</script>

</body>
</html>
EOF
```

### 19.6 Test the Socket.IO integration

Restart FastAPI using the wrapped app:

```bash
uvicorn app.main:socket_app --host 0.0.0.0 --port 8000
```

Open:

```text
http://<VM-IP>:8000/socketio
```

Click **Start Task**. The Socket.IO client connects, joins the task's room, and the progress bar fills the same way as the native-WebSocket version — but now with automatic reconnection and room-based isolation between tasks.

### 19.7 Architecture recap

```text
Celery Worker
     │  publish
     ▼
Redis Pub/Sub (db=1)
     │  subscribed by
     ▼
python-socketio listener (per task_id room)
     │  sio.emit("progress", ..., room=task_id)
     ▼
Socket.IO client (browser)
```

The raw WebSocket endpoint (`/ws/{task_id}`) from Section 10 still works unchanged — Socket.IO was added alongside it, not instead of it, so both approaches can be compared on the same backend.

## 20. Troubleshooting

### FastAPI is not accessible

Start FastAPI using:

```bash
uvicorn app.main:socket_app --host 0.0.0.0 --port 8000
```

Make sure port `8000` is exposed by the Poridhi VM.

### WebSocket / Socket.IO connection error

Open the application using the same FastAPI host and port:

```text
http://<VM-IP>:8000
```

Accessing it via `127.0.0.1` from outside the VM will only work if you have a port-forwarded SSH tunnel — otherwise use the VM's exposed URL.

### Socket.IO client connects but never receives events

Confirm `app/socketio_events.py` was actually imported (Section 19.4) — if `socketio_events` is never imported, the `@sio.event` handlers never register and `subscribe` does nothing. Also confirm you're running `app.main:socket_app`, not `app.main:app`.

### Pub/Sub messages aren't reaching either client

Double-check that `tasks.py`, `main.py`, and `socketio_events.py` all use `db=1` for the Redis Pub/Sub client, while Celery's broker/backend stay on `db=0`.

## 21. Expected Output

### Celery Worker

```text
[tasks]
  . process_task

celery@... ready.
```

After starting a task:

```text
[INFO] Task app.tasks.process_task[abc123] received
[INFO] Task app.tasks.process_task[abc123] succeeded
```

### FastAPI

```text
Uvicorn running on http://0.0.0.0:8000
```

### Task Creation

```json
{
    "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "status": "queued",
    "websocket": "/ws/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

### Real-Time Progress (native WebSocket and Socket.IO clients both show)

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

## Conclusion

In this lab, you built a real-time event streaming system using **Celery, Redis Pub/Sub, and FastAPI WebSocket**, then extended it with a **production-grade Socket.IO integration** on top of the same backend. The system delivers background task progress to users instantly without polling or page refreshes, and demonstrates two interchangeable ways — raw WebSocket and Socket.IO — to deliver that same real-time stream to the browser.
