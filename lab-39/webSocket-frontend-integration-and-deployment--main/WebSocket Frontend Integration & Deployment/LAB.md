# Lab: WebSocket Frontend Integration & Deployment

## Introduction

Real-time systems let users see task results and progress as soon as they are produced.
In this lab, Celery workers publish task events through Redis Pub/Sub channels.
A Python Socket.IO server forwards these events to connected clients for live updates.
Finally, the complete system is deployed behind Nginx with WebSocket support on Poridhi Cloud.


### System overview

```
Browser (index.html, Socket.IO client)
        │  HTTP POST /tasks
        ▼
Flask API  ──► Celery worker ──► Redis pub/sub  (channel: task:<id>)
                   │                   ▲
                   │                   │
                   └─► Redis broker ◄──┘
                                       │
WebSocket server (python-socketio) ◄───┘
        │  Socket.IO room: task_<id>
        ▼
Browser renders live updates
```

### End-to-end message sequence

```mermaid
sequenceDiagram
    autonumber
    participant U as User browser
    participant F as Flask API
    participant W as Celery worker
    participant R as Redis
    participant S as WebSocket server

    U->>F: POST /tasks {payload, fail_probability}
    F-->>U: 202 {task_id}
    U->>S: socket.emit('subscribe_task', {task_id})
    S->>R: SUBSCRIBE task:<id>

    W->>R: PUBLISH task:<id> {state: STARTED}
    R-->>S: message
    S-->>U: task_update {state: STARTED}

    loop for each progress step
        W->>R: PUBLISH task:<id> {state: PROGRESS, progress}
        R-->>S: message
        S-->>U: task_update {state: PROGRESS}
    end

    W->>R: PUBLISH task:<id> {state: SUCCESS, result}
    R-->>S: message
    S-->>U: task_update {state: SUCCESS}

### Why pub/sub, not Celery events

* Celery events are an internal monitoring stream; they are designed for Flower-style tooling, not for end-user UIs.
* Per-task Redis channels give us a clean, stateless routing key — exactly one publisher, exactly the interested subscribers.


## 2. Objectives 

By the end of this lab you will:

1. Publish Celery task lifecycle events to a Redis channel `task:<id>`.
2. Bridge those events to the right Socket.IO room via `python-socketio`.
3. Serve a small HTML/JS frontend that submits tasks and renders the live stream.
4. Deploy the full stack behind nginx on Poridhi Cloud with WebSocket upgrade support.

## Project structure
websocket-realtime-lab/
├── requirements.txt
├── celery_app.py
├── tasks.py
├── app.py
├── ws_server.py
├── scripts/
│   ├── start_redis.sh
│   ├── start_worker.sh
│   ├── start_api.sh
│   ├── start_ws.sh
│   └── stop_all.sh
├── static/
│   └── index.html
├── nginx/
│   └── websocket.conf
└── systemd/
    ├── celery-worker.service
    ├── flask-api.service
    └── ws-server.service
```

---

## 3. Environment Setup & Prerequisites

Install system prerequisites:

```bash
sudo apt update
sudo apt install -y nginx redis-server
```

Create the venv and install dependencies:

```bash
cd websocket-realtime-lab
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 4. Step-by-Step Implementation

### Step 4.1 — `requirements.txt`

```text
flask==3.0.3
celery==5.4.0
redis==5.0.8
flower==2.0.1
python-socketio[asyncio]==5.11.3
uvicorn[standard]==0.30.6
gunicorn==22.0.0
```

Reuses `flask`, `celery`, `redis`, `flower` from the prior lab. Adds the Socket.IO server, an ASGI host, and a production WSGI server for the API.

### Step 4.2 — `celery_app.py`

```python
from celery import Celery

celery_app = Celery(
    "websocket_realtime_lab",
    broker="redis://127.0.0.1:6379/0",
    backend="redis://127.0.0.1:6379/0",
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    worker_send_task_events=True,
)

celery_app.autodiscover_tasks(["tasks"])
```

Identical to the prior lab's Celery bootstrap — we just reuse Redis DB 0 as both broker and pub/sub channel.

### Step 4.3 — `tasks.py`

```python
import json, random, time
from datetime import datetime, timezone
import redis
from celery.utils.log import get_task_logger
from celery_app import celery_app

logger = get_task_logger(__name__)
_redis = redis.Redis.from_url("redis://127.0.0.1:6379/0", decode_responses=True)

def publish_state(task_id, state, **extra):
    payload = {
        "state": state,
        "task_id": task_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    _redis.publish(f"task:{task_id}", json.dumps(payload))

@celery_app.task(bind=True, name="tasks.process_order",
                  autoretry_for=(RuntimeError,),
                  retry_backoff=True,
                  retry_kwargs={"max_retries": 3})
def process_order(self, payload, fail_probability=0.0):
    task_id = self.request.id
    publish_state(task_id, "STARTED", payload=payload)

    for step in range(1, 6):
        time.sleep(0.5)
        publish_state(task_id, "PROGRESS", payload=payload,
                      progress=step, total=5,
                      message=f"step {step}/5 complete")

    if random.random() < fail_probability:
        publish_state(task_id, "FAILURE", error="simulated failure")
        raise RuntimeError("simulated failure")

    result = {"payload": payload, "status": "completed", "by": "celery"}
    publish_state(task_id, "SUCCESS", result=result)
    return result
```

> **Why explicit `publish_state` calls?**
> Celery signals also work, but they couple your task to Celery internals and your UI to those exact signals. Publishing from inside the task gives you total control over payload shape and lets you verify with `redis-cli SUBSCRIBE "task:*"`.

### Step 4.4 — `ws_server.py`

```python
import asyncio, json, os
import redis.asyncio as redis_async
import socketio

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*")
app = socketio.ASGIApp(sio)
_redis = redis_async.from_url(REDIS_URL, decode_responses=True)

_subscribers, _refcounts, _locks = {}, {}, {}

def _lock_for(tid):
    if tid not in _locks: _locks[tid] = asyncio.Lock()
    return _locks[tid]

async def _pump(task_id):
    room, channel = f"task_{task_id}", f"task:{task_id}"
    pubsub = _redis.pubsub()
    await pubsub.subscribe(channel)
    async for msg in pubsub.listen():
        if msg.get("type") != "message": continue
        try: payload = json.loads(msg["data"])
        except Exception: payload = {"raw": msg["data"]}
        await sio.emit("task_update", payload, room=room)
    await pubsub.unsubscribe(channel); await pubsub.close()

@sio.event
async def connect(sid, environ, auth):
    pass

@sio.on("subscribe_task")
async def on_subscribe(sid, data):
    task_id = (data or {}).get("task_id")
    if not task_id:
        await sio.emit("task_update", {"state": "ERROR", "error": "task_id required"}, to=sid)
        return
    async with _lock_for(task_id):
        _refcounts[task_id] = _refcounts.get(task_id, 0) + 1
        await sio.enter_room(sid, f"task_{task_id}")
        if task_id not in _subscribers or _subscribers[task_id].done():
            _subscribers[task_id] = asyncio.create_task(_pump(task_id))

@sio.on("unsubscribe_task")
async def on_unsubscribe(sid, data):
    tid = (data or {}).get("task_id")
    if not tid: return
    async with _lock_for(tid):
        await sio.leave_room(sid, f"task_{tid}")
        _refcounts[tid] = max(0, _refcounts.get(tid, 0) - 1)
        if _refcounts[tid] == 0 and tid in _subscribers:
            _subscribers[tid].cancel()
            _subscribers.pop(tid, None)
            _refcounts.pop(tid, None)
```

**Routing rule:** `subscribe_task { task_id: "..." }` → server joins room `task_<id>` and ensures one (and only one) Redis subscriber is running for that channel.

### Step 4.5 — `app.py`

```python
import os
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from tasks import process_order

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app = Flask(__name__, static_folder=None)
CORS(app)

@app.route("/", methods=["GET"])
def index():
    return send_from_directory(STATIC_DIR, "index.html")

@app.route("/health", methods=["GET"])
def health():
    return jsonify(status="ok")

@app.route("/tasks", methods=["POST"])
def submit_task():
    body = request.get_json(silent=True) or {}
    payload = body.get("payload", "demo")
    fail_probability = float(body.get("fail_probability", 0.0))
    r = process_order.delay(payload, fail_probability)
    return jsonify(task_id=r.id, payload=payload,
                   fail_probability=fail_probability), 202
```

The endpoint returns `task_id` — the only thing the frontend needs to call `subscribe_task` with.

### Step 4.6 — `static/index.html`

```html
<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
  const socket = io({ transports: ["websocket", "polling"] });
  socket.on("task_update", (p) => console.log(p));

  async function submit(payload, fail_probability) {
    const res = await fetch("/tasks", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ payload, fail_probability }),
    });
    const { task_id } = await res.json();
    socket.emit("subscribe_task", { task_id });
    return task_id;
  }
</script>
<form onsubmit="event.preventDefault(); submit(this.payload.value, +this.fp.value)">
  <input name="payload" value="order-2001" />
  <input name="fp" type="number" value="0" min="0" max="1" step="0.05" />
  <button>Submit</button>
</form>
```

(The full file in the repo adds badges, timestamps, and a connection indicator; the snippet above is the load-bearing logic.)

### Step 4.7 — `scripts/start_*.sh`

| Script | Purpose |
|--------|---------|
| `start_redis.sh`  | `systemctl enable --now redis-server` (no-op if already running). |
| `start_worker.sh` | `celery -A celery_app.celery_app worker --loglevel=info --concurrency=2 -E`. |
| `start_api.sh`    | `gunicorn --bind 0.0.0.0:5000 app:app`. |
| `start_ws.sh`     | `uvicorn ws_server:app --host 0.0.0.0 --port 5556`. |
| `stop_all.sh`     | `pkill -f` each service for quick teardown. |

Each script `set -euo pipefail`, `cd`s to the repo root, and `source venv/bin/activate` before exec'ing the target binary.

### Step 4.8 — `nginx/websocket.conf`

```nginx
upstream flask_api       { server 127.0.0.1:5000; }
upstream socketio_server { server 127.0.0.1:5556; }

server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://flask_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /socket.io/ {
        proxy_pass http://socketio_server/socket.io/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 600s;
    }
}
```

Enable and reload:

```bash
sudo ln -sf "$(pwd)/nginx/websocket.conf" /etc/nginx/sites-enabled/websocket.conf
sudo nginx -t && sudo systemctl reload nginx
```

> The `Upgrade` + `Connection: upgrade` headers are the only magic — without them nginx treats the WebSocket handshake as plain HTTP and the socket closes immediately.

### Step 4.9 — `systemd/*.service`

Three unit files (`celery-worker.service`, `flask-api.service`, `ws-server.service`) share the same shape: `Type=simple`, `User=www-data`, `WorkingDirectory=/opt/websocket-realtime-lab`, `ExecStart=/opt/.../venv/bin/<binary>`, `Restart=always`.

`ws-server.service` adds `After=redis-server.service Wants=redis-server.service` so it never starts before its dependency. Deploy with:

```bash
sudo cp systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now celery-worker flask-api ws-server
```

### Step 4.10 — Poridhi Cloud deployment

1. Provision a Poridhi Cloud instance (Ubuntu 22.04, ≥ 1 GB RAM).
2. `git clone` the repo into `/opt/websocket-realtime-lab`.
3. Install Redis + nginx, create the venv, install requirements.
4. Copy the three `systemd/*.service` files into `/etc/systemd/system/`.
5. Symlink `nginx/websocket.conf` into `/etc/nginx/sites-enabled/`.
6. `sudo systemctl enable --now redis-server celery-worker flask-api ws-server nginx`.
7. Open firewall for 80 (or 443) and point your DNS / Poridhi edge at the instance IP.

### Step 4.11 — AWS variant (alternative)

* Launch an EC2 instance (`t3.small`, Ubuntu 22.04 AMI), open security-group ingress for 22 + 80 (and 443).
* SSH in, then follow the same `apt install`, venv, `systemctl` steps as 4.10.
* Optional: terminate TLS with `certbot --nginx -d your-domain`.
* Optional: front the instance with an ALB and let it forward 80 to nginx — no extra config needed for WebSockets as long as the ALB target group has `stickiness` enabled (or use sticky sessions on the Socket.IO polling fallback).

---

## 5. Execution & Verification

### Bring it up locally

```bash
./scripts/start_redis.sh
./scripts/start_worker.sh &
./scripts/start_api.sh   &
./scripts/start_ws.sh    &
```

### Verification matrix

| # | Action | Expected |
|---|--------|----------|
| 1 | `redis-cli SUBSCRIBE 'task:*'` in one shell; submit task from browser | JSON messages arrive on `task:<id>` for each `STARTED`, `PROGRESS`, `SUCCESS`. |
| 2 | DevTools → Network → WS frames | Frames named `task_update` carrying the same payloads. |
| 3 | Submit task with `fail_probability=0.5` (triggers retries) | UI shows `STARTED → PROGRESS → RETRY → STARTED → … → SUCCESS`. |
| 4 | `curl -i "http://localhost/socket.io/?EIO=4&transport=polling"` | `HTTP/1.1 200 OK` polling response, then upgrade on second call (`HTTP/1.1 101 Switching Protocols`). |
| 5 | `curl -s -o /dev/null -w "%{http_code}\n" http://localhost/` | `200` (nginx → Flask → index.html). |
| 6 | `systemctl is-active celery-worker flask-api ws-server` | `active` for all three. |
| 7 | AWS variant | Same checks, from a remote browser. |

### End-to-end walkthrough

1. Open `http://<host>/` in a browser.
2. Accept defaults (`payload = order-2001`, `fail_probability = 0`), click **Submit**.
3. The console below the form fills with badges (`STARTED`, `PROGRESS x5`, `SUCCESS`) — every line stamped with the same `task_id`.
4. Re-submit with `fail_probability = 0.8` — you'll see `FAILURE` followed by another `STARTED` once Celery retries.

### Teardown

```bash
./scripts/stop_all.sh
```

---

## Conclusion

You wired the full real-time stack:

* Celery worker publishes lifecycle events to a **per-task Redis pub/sub channel**.
* A **python-socketio** server subscribes to those channels and forwards messages into **Socket.IO rooms** keyed by task ID — with refcounted background tasks so we never leak Redis subscribers.
* A **single static HTML page** with the Socket.IO client renders the live stream and gives users instant feedback instead of polling.
* The whole stack is deployed behind **nginx with WebSocket upgrade headers**, ready to run on Poridhi Cloud (or AWS EC2 / ALB) in production.
