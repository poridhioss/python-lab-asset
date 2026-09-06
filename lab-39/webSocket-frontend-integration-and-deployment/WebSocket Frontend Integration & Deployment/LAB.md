# Lab: WebSocket Frontend Integration & Deployment

## Introduction

Real-time systems let users see task results and progress as soon as they are produced. In this lab, Celery workers publish task events through Redis Pub/Sub channels. A Python Socket.IO server forwards these events to connected clients for live updates. Finally, the complete system is deployed behind Nginx with WebSocket support on Poridhi Cloud.

### System overview

![Lab 39 Architecture](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39architecture.png)

**svg**

### End-to-end message sequence

![Lab 39 Flow Diagram](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39flowdiagram.png)

**Unable to render rich display**

Parse error on line 30:
...al monitoring stream; they are designed
\-----------------------^
Expecting '()', 'SOLID\_OPEN\_ARROW', 'DOTTED\_OPEN\_ARROW', 'SOLID\_ARROW', 'SOLID\_ARROW\_TOP', 'SOLID\_ARROW\_BOTTOM', 'STICK\_ARROW\_TOP', 'STICK\_ARROW\_BOTTOM', 'SOLID\_ARROW\_TOP\_DOTTED', 'SOLID\_ARROW\_BOTTOM\_DOTTED', 'STICK\_ARROW\_TOP\_DOTTED', 'STICK\_ARROW\_BOTTOM\_DOTTED', 'SOLID\_ARROW\_TOP\_REVERSE', 'SOLID\_ARROW\_BOTTOM\_REVERSE', 'STICK\_ARROW\_TOP\_REVERSE', 'STICK\_ARROW\_BOTTOM\_REVERSE', 'SOLID\_ARROW\_TOP\_REVERSE\_DOTTED', 'SOLID\_ARROW\_BOTTOM\_REVERSE\_DOTTED', 'STICK\_ARROW\_TOP\_REVERSE\_DOTTED', 'STICK\_ARROW\_BOTTOM\_REVERSE\_DOTTED', 'BIDIRECTIONAL\_SOLID\_ARROW', 'DOTTED\_ARROW', 'BIDIRECTIONAL\_DOTTED\_ARROW', 'SOLID\_CROSS', 'DOTTED\_CROSS', 'SOLID\_POINT', 'DOTTED\_POINT', got 'NEWLINE'

For more information, see https\://docs.github.com/get-started/writing-on-github/working-with-advanced-formatting/creating-diagrams#creating-mermaid-diagrams


```
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


## 1.1 Lab readiness note

> This document is written to be VM-test-ready. Before distribution, replace `<YOUR_LAB_REPOSITORY_URL>` with the real repository URL and ensure the repository contains every file listed in the project structure.

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
cd ~/code
mkdir -p websocket-realtime-lab/{scripts,static,nginx,systemd}
cd websocket-realtime-lab
---
---
touch requirements.txt celery_app.py tasks.py app.py ws_server.py
touch scripts/start_redis.sh scripts/start_worker.sh scripts/start_api.sh scripts/start_ws.sh scripts/stop_all.sh
touch static/index.html
touch nginx/websocket.conf
touch systemd/celery-worker.service systemd/flask-api.service systemd/ws-server.service
---
## 3. Environment Setup & Prerequisites


Install system prerequisites:

```
sudo apt update
```
![Output 1](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output1.png)

```
sudo apt install -y nginx redis-server
```

![Output 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output2.png)


Create the venv and install dependencies:

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
![Output 3](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output3.png)

---

## 4. Step-by-Step Implementation


### Step 4.1 — `requirements.txt`


```
flask==3.0.3
celery==5.4.0
redis==5.0.8
flower==2.0.1
python-socketio[asyncio]==5.11.3
uvicorn[standard]==0.30.6
gunicorn==22.0.0

```
pip install "flask==3.0.3" "celery==5.4.0" "redis==5.0.8" "flower==2.0.1" "python-socketio[asyncio]==5.11.3" "uvicorn[standard]==0.30.6" "gunicorn==22.0.0"

```
![Output 4](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output4.png)


Reuses `flask`, `celery`, `redis`, `flower` from the prior lab. Adds the Socket.IO server, an ASGI host, and a production WSGI server for the API.

### Step 4.2 — `celery_app.py`

[svg]

```
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

### Step 4.3 — `tasks.py`


```
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


> **Why explicit ****`publish_state`**** calls?** Celery signals also work, but they couple your task to Celery internals and your UI to those exact signals. Publishing from inside the task gives you total control over payload shape and lets you verify with `redis-cli PSUBSCRIBE "task:*"`.

### Step 4.4 — `ws_server.py`


```
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


**Routing rule:** `subscribe_task { task_id: "..." }` → server joins room `task_<id>` and ensures one (and only one) Redis subscriber is running for that channel.

### Step 4.5 — `app.py`


```
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

The endpoint returns `task_id` — the only thing the frontend needs to call `subscribe_task` with.

### Step 4.6 — `static/index.html`


```
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

### Step 4.7 — `scripts/start_*.sh`


| **ScriptPurpose** |                                                                              |
| ----------------- | ---------------------------------------------------------------------------- |
| `start_redis.sh`  | `systemctl enable --now redis-server` (no-op if already running).            |
| `start_worker.sh` | `celery -A celery_app.celery_app worker --loglevel=info --concurrency=2 -E`. |
| `start_api.sh`    | `gunicorn --bind 0.0.0.0:5000 app:app`.                                      |
| `start_ws.sh`     | `uvicorn ws_server:app --host 0.0.0.0 --port 5556`.                          |
| `stop_all.sh`     | `pkill -f` each service for quick teardown.                                  |

Each script `set -euo pipefail`, `cd`s to the repo root, and `source venv/bin/activate` before exec'ing the target binary.

### Step 4.8 — `nginx/websocket.conf`


```
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
 
sudo systemctl start nginx
sudo systemctl status nginx --no-pager
sudo systemctl enable nginx
sudo nginx -t && sudo systemctl reload nginx

```

sudo nginx -t && sudo systemctl reload nginx
```
![Output 5](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output5.png)

> The `Upgrade` + `Connection: upgrade` headers are the only magic — without them nginx treats the WebSocket handshake as plain HTTP and the socket closes immediately.

### Step 4.9 — `systemd/*.service`


Three unit files (`celery-worker.service`, `flask-api.service`, `ws-server.service`) share the same shape: `Type=simple`, `User=www-data`, `WorkingDirectory=/opt/websocket-realtime-lab`, `ExecStart=/opt/.../venv/bin/<binary>`, `Restart=always`.

`ws-server.service` adds `After=redis-server.service Wants=redis-server.service` so it never starts before its dependency. Deploy with:

```
ls -lh systemd/*.service

sudo cp systemd/*.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl reset-failed celery-worker flask-api ws-server
sudo systemctl enable celery-worker flask-api ws-server
sudo systemctl start celery-worker flask-api ws-server
```
sudo systemctl is-active celery-worker
sudo systemctl is-active flask-api
sudo systemctl is-active ws-server
```


![Output 6](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output6.png)



### Step 4.9A — Manual smoke test before systemd

Run these in separate SSH/terminal sessions. This isolates application problems from systemd/Nginx problems.

**Terminal 1 — Redis**

```bash
sudo systemctl start redis-server
sudo systemctl enable redis-server
sudo systemctl status redis-server --no-pager
redis-cli ping
```
![Output 7](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output7.png)

Expected:

```text
PONG
```

**Terminal 2 — Celery worker**

```

cd ~/code/websocket-realtime-lab
source venv/bin/activate
celery -A celery_app:celery worker --loglevel=info --concurrency=2 -E

```
![Output 8](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output8.png)

Expected: the worker registers `tasks.process_order`.


**Terminal 3 — Socket.IO server**

```bash
cd ~/code/websocket-realtime-lab
source venv/bin/activate
uvicorn ws_server:app --host 127.0.0.1 --port 5556

```
![Output 9](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output9.png)

**Terminal 4 — Redis event monitor**

```bash
redis-cli PSUBSCRIBE 'task:*'
```
![Output 10](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output10.png)

Then submit a task:

```bash
cd ~/code/websocket-realtime-lab
source venv/bin/activate
curl -s -X POST "http://127.0.0.1:8000/tasks?filename=order-2001"
```

![Output 11](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output11.png)

![Output 12](https://raw.githubusercontent.com/poridhioss/python-lab-asset/00702a9cde54ea93efdec8c70fbbebbe62492f22/lab-39output12.png)

Expected response contains a `task_id` and HTTP status `queued`.

You should then see `STARTED`, five `PROGRESS` messages, and `SUCCESS` in the Redis monitor.

### Step 4.10 —  Cloud deployment

svg

1. Provision a Poridhi Cloud instance (Ubuntu 22.04, ≥ 1 GB RAM).
2. `git clone` the repo into `/opt/websocket-realtime-lab`.
3. Install Redis + nginx, create the venv, install requirements.
4. Copy the three `systemd/*.service` files into `/etc/systemd/system/`.
5. Symlink `nginx/websocket.conf` into `/etc/nginx/sites-enabled/`.
6. `sudo systemctl enable --now redis-server celery-worker flask-api ws-server nginx`.
7. Open firewall for 80 (or 443) and point your DNS / Poridhi edge at the instance IP.

### Step 4.11 — AWS variant (alternative)

[svg](https://github.com/poridhioss/-Real-Time-Systems-Modules-69-72/blob/main/lab-39/webSocket-frontend-integration-and-deployment--main/WebSocket%20Frontend%20Integration%20%26%20Deployment/LAB.md#step-411--aws-variant-alternative)

- Launch an EC2 instance (`t3.small`, Ubuntu 22.04 AMI), open security-group ingress for 22 + 80 (and 443).
- SSH in, then follow the same `apt install`, venv, `systemctl` steps as 4.10.
- Optional: terminate TLS with `certbot --nginx -d your-domain`.
- Optional: front the instance with an ALB and let it forward 80 to nginx — no extra config needed for WebSockets as long as the ALB target group has `stickiness` enabled (or use sticky sessions on the Socket.IO polling fallback).

---

```
./scripts/start_redis.sh
./scripts/start_worker.sh &
./scripts/start_api.sh   &
./scripts/start_ws.sh    &
```

**svg**

|**#ActionExpected** |                                                                       |                                                                                                       |
| ------------------- | --------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| 1                   | `redis-cli PSUBSCRIBE 'task:*'` in one shell; submit task from browser | JSON messages arrive on `task:<id>` for each `STARTED`, `PROGRESS`, `SUCCESS`.                        |
| 2                   | DevTools → Network → WS frames                                        | Frames named `task_update` carrying the same payloads.                                                |
| 3                   | Submit task with `fail_probability=0.5` (triggers retries)            | UI shows `STARTED → PROGRESS → RETRY → STARTED → … → SUCCESS`.                                        |
| 4                   | `curl -i "http://localhost/socket.io/?EIO=4&transport=polling"`       | `HTTP/1.1 200 OK` polling response, then upgrade on second call (`HTTP/1.1 101 Switching Protocols`). |
| 5                   | `curl -s -o /dev/null -w "%{http_code}\n" http://localhost/`          | `200` (nginx → Flask → index.html).                                                                   |
| 6                   | `systemctl is-active celery-worker flask-api ws-server`               | `active` for all three.                                                                               |
| 7                   | AWS variant                                                           | Same checks, from a remote browser.                                                                   |


1. Open `http://<host>/` in a browser.
2. Accept defaults (`payload = order-2001`, `fail_probability = 0`), click **Submit**.
3. The console below the form fills with badges (`STARTED`, `PROGRESS x5`, `SUCCESS`) — every line stamped with the same `task_id`.
4. Re-submit with `fail_probability = 0.8` — you'll see `FAILURE` followed by another `STARTED` once Celery retries.


Check the listening ports:

```bash
sudo ss -lntp | grep -E ':80|:5000|:5556'
```

Only port **80** needs to be exposed publicly when Nginx is used as the reverse proxy. Ports **5000** and **5556** should remain internal.



```
./scripts/stop_all.sh

---

## Conclusion

- Celery worker publishes lifecycle events to a **per-task Redis pub/sub channel**.
- A **python-socketio** server subscribes to those channels and forwards messages into **Socket.IO rooms** keyed by task ID — with refcounted background tasks so we never leak Redis subscribers.
- A **single static HTML page** with the Socket.IO client renders the live stream and gives users instant feedback instead of polling.
- The whole stack is deployed behind **nginx with WebSocket upgrade headers**, ready to run on Poridhi Cloud (or AWS EC2 / ALB) in production.