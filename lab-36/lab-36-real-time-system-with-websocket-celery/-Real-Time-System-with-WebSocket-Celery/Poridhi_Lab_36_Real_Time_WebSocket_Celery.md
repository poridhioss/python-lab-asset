# Real-Time System with WebSocket + Celery

 Async Task Processing with Celery


## Introduction

Long-running tasks can slow down web applications when handled synchronously. **Celery** moves these tasks to background workers, while **Redis** handles task messaging and real-time progress events.
In this lab, you will build a real-time task processing system using **FastAPI, Celery, Redis, WebSockets, and Flower**, allowing users to submit tasks, view live progress, and monitor workers.


## What You Will Learn

* Configure Celery with Redis for background task processing
* Track and stream real-time task progress using Redis Pub/Sub and WebSockets
* Build a browser-based live progress UI
* Monitor tasks with Flower and run the stack using Docker Compose


## Architecture Diagram

The system contains a FastAPI web service, a Celery worker, Flower, and one Redis instance using three logical databases.

```text
                         ┌──────────────────────────┐
                         │       Web Browser        │
                         │   Live progress bar UI   │
                         └─────────────┬────────────┘
                                       │
                         HTTP POST     │     WebSocket
                         /submit-task  │     /ws/{task_id}
                                       │
                                       ▼
                         ┌──────────────────────────┐
                         │        FastAPI            │
                         │       :8000               │
                         │                           │
                         │ REST + WebSocket bridge   │
                         └───────┬───────────┬───────┘
                                 │           │
                            enqueue      subscribe
                                 │           │
                                 ▼           ▼
                    ┌────────────────┐   ┌──────────────────┐
                    │ Redis Broker   │   │ Redis Pub/Sub    │
                    │     DB 0       │   │      DB 2        │
                    └───────┬────────┘   └────────▲─────────┘
                            │                     │ publish
                            ▼                     │
                    ┌────────────────────────────────────┐
                    │          Celery Worker             │
                    │                                    │
                    │ executes process_task              │
                    │ updates state + publishes progress │
                    └───────────────┬────────────────────┘
                                    │
                              writes result
                                    ▼
                    ┌────────────────────────────────────┐
                    │       Redis Result Backend         │
                    │              DB 1                  │
                    └───────────────┬────────────────────┘
                                    │
                               reads state
                                    ▼
                    ┌────────────────────────────────────┐
                    │       Flower Dashboard :5555       │
                    └────────────────────────────────────┘


### Data Flow

1. The browser sends `POST /submit-task`.
2. FastAPI queues a Celery task and immediately returns a `task_id`.
3. The browser opens a WebSocket connection using that `task_id`.
4. The Celery worker executes the task.
5. The worker publishes progress events to Redis DB 2.
6. FastAPI subscribes to the task-specific Redis channel.
7. FastAPI forwards progress events to the browser through WebSocket.
8. Celery stores task state and results in Redis DB 1.
9. Flower reads Celery events and displays worker/task information.



## Why Three Redis Databases?

A single Redis instance can contain multiple logical databases. In this lab, each database has a separate responsibility.

| Database | Role | Used By |
|---|---|---|
| `db0` | Celery broker | FastAPI/Celery client + worker |
| `db1` | Celery result backend | Worker + `AsyncResult` |
| `db2` | Redis Pub/Sub | Worker publishes + FastAPI subscribes |

Separating these responsibilities makes the architecture easier to understand and inspect.



## Prerequisites

Before starting this lab, make sure you have:

- Docker installed
- Docker Compose installed
- Basic Python knowledge
- Basic understanding of REST APIs
- Basic understanding of asynchronous/background tasks
- A Poridhi lab VM or Linux environment
- Ports `8000` and `5555` available



# Task 01 — Set Up the Project Structure

Create the project directory:

```bash
mkdir realtime-celery-lab
cd realtime-celery-lab
mkdir static
```

Create the required files:

```bash
touch main.py celery_app.py tasks.py requirements.txt Dockerfile docker-compose.yml
touch static/index.html
```

Your final project structure should look like this:

```text
realtime-celery-lab/
├── main.py
├── celery_app.py
├── tasks.py
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
└── static/
    └── index.html
```


# Task 02 — Configure Python Dependencies

Open `requirements.txt`:

```bash
nano requirements.txt
```

Add:

```text
fastapi
uvicorn[standard]
celery
redis
flower
```

Save the file.

These packages provide:

- `fastapi` — REST API and WebSocket server
- `uvicorn` — ASGI server
- `celery` — background task processing
- `redis` — Redis client and Celery Redis transport
- `flower` — Celery monitoring dashboard

---

# Task 03 — Configure Celery

Create `celery_app.py`:

```python
import os

from celery import Celery


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")

BROKER_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"
BACKEND_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/1"


celery_app = Celery(
    "realtime_tasks",
    broker=BROKER_URL,
    backend=BACKEND_URL,
    include=["tasks"],
)


celery_app.conf.update(
    task_track_started=True,
    result_expires=3600,
    worker_send_task_events=True,
    task_send_sent_event=True,
)


### Explanation

The Celery broker uses Redis DB 0:

```text
redis://redis:6379/0
```

The result backend uses Redis DB 1:

```text
redis://redis:6379/1
```

The environment variables allow the same Python code to work both locally and inside Docker Compose.



# Task 04 — Create the Celery Task

Open `tasks.py`:

```python
import json
import os
import time

import redis

from celery_app import celery_app


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=2,
    decode_responses=True,
)


def publish_progress(task_id: str, payload: dict):
    channel = f"task-progress:{task_id}"
    redis_client.publish(channel, json.dumps(payload))


@celery_app.task(bind=True, name="tasks.process_task")
def process_task(
    self,
    total_steps: int = 10,
    step_delay: float = 1.0,
):
    task_id = self.request.id

    started_payload = {
        "state": "STARTED",
        "current": 0,
        "total": total_steps,
        "percent": 0.0,
    }

    publish_progress(task_id, started_payload)

    try:
        for step in range(1, total_steps + 1):
            time.sleep(step_delay)

            progress = {
                "state": "PROGRESS",
                "current": step,
                "total": total_steps,
                "percent": round((step / total_steps) * 100, 1),
            }

            self.update_state(
                state="PROGRESS",
                meta=progress,
            )

            publish_progress(task_id, progress)

        result = {
            "state": "SUCCESS",
            "current": total_steps,
            "total": total_steps,
            "percent": 100.0,
        }

        publish_progress(task_id, result)

        return {
            "message": f"Task {task_id} completed",
            "steps_completed": total_steps,
        }

    except Exception as exc:
        failure = {
            "state": "FAILURE",
            "current": 0,
            "total": total_steps,
            "percent": 0.0,
            "error": str(exc),
        }

        publish_progress(task_id, failure)
        raise
```

### How It Works

Every progress update is handled in two ways:

1. Celery updates its own task state.
2. The same progress information is published to Redis Pub/Sub DB 2.

The task-specific channel is:

```text
task-progress:<task_id>
```

For example:

```text
task-progress:9c3e6b8a-...
```

This ensures that multiple tasks can run at the same time without mixing their progress events.


# Task 05 — Build the FastAPI + WebSocket Bridge

Open `main.py`:

```python
import asyncio
import json
import os

import redis.asyncio as aioredis
from celery.result import AsyncResult
from fastapi import FastAPI, WebSocket
from fastapi.staticfiles import StaticFiles

from celery_app import celery_app
from tasks import process_task


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))


app = FastAPI(title="Real-Time Celery + WebSocket Lab")

app.mount(
    "/static",
    StaticFiles(directory="static"),
    name="static",
)


@app.get("/")
def root():
    return {
        "message": "Real-Time Celery + WebSocket Lab",
        "ui": "/static/index.html",
    }


@app.post("/submit-task")
def submit_task(
    total_steps: int = 10,
    step_delay: float = 1.0,
):
    task = process_task.delay(
        total_steps=total_steps,
        step_delay=step_delay,
    )

    return {"task_id": task.id}


@app.get("/task-status/{task_id}")
def task_status(task_id: str):
    result = AsyncResult(task_id, app=celery_app)

    return {
        "task_id": task_id,
        "state": result.state,
        "result": result.result,
    }


@app.websocket("/ws/{task_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    task_id: str,
):
    await websocket.accept()

    redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=2,
        decode_responses=True,
    )

    pubsub = redis_client.pubsub()
    channel = f"task-progress:{task_id}"

    await pubsub.subscribe(channel)

    try:
        # Check the current Celery state after subscribing.
        # This prevents a race where the task finishes before
        # the WebSocket connection is established.
        result = AsyncResult(task_id, app=celery_app)

        if result.state in {"SUCCESS", "FAILURE"}:
            payload = {
                "state": result.state,
                "current": 0,
                "total": 0,
                "percent": 100.0 if result.state == "SUCCESS" else 0.0,
            }

            if isinstance(result.result, dict):
                payload.update(result.result)

            await websocket.send_json(payload)
            return

        if result.state == "PROGRESS" and isinstance(result.result, dict):
            await websocket.send_json(result.result)

        while True:
            message = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=1.0,
            )

            if message is not None:
                data = json.loads(message["data"])

                await websocket.send_json(data)

                if data.get("state") in {"SUCCESS", "FAILURE"}:
                    break
            else:
                # Re-check Celery state periodically in case the
                # terminal Pub/Sub event happened before subscription.
                current = AsyncResult(task_id, app=celery_app)

                if current.state in {"SUCCESS", "FAILURE"}:
                    payload = {
                        "state": current.state,
                        "current": 0,
                        "total": 0,
                        "percent": 100.0
                        if current.state == "SUCCESS"
                        else 0.0,
                    }

                    if isinstance(current.result, dict):
                        payload.update(current.result)

                    await websocket.send_json(payload)
                    break

                await asyncio.sleep(0.1)

    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()


### Why This WebSocket Implementation Is Safer

Redis Pub/Sub does not store old messages.

Therefore, a simple implementation can lose the final `SUCCESS` event if the task completes before the browser subscribes.

This implementation avoids that problem by:

1. Subscribing to Redis first.
2. Checking the current Celery state.
3. Sending the current state if the task is already in progress.
4. Listening for future Pub/Sub events.
5. Re-checking Celery state if no Pub/Sub message arrives.
6. Closing the WebSocket after `SUCCESS` or `FAILURE`.

This makes the lab reliable even when a task finishes very quickly.



# Task 06 — Build the Browser UI

Open `static/index.html`:

```html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Real-Time Celery Lab</title>

  <style>
    body {
      font-family: system-ui, sans-serif;
      max-width: 720px;
      margin: 2rem auto;
      padding: 0 1rem;
    }

    .row {
      display: flex;
      gap: 1rem;
      margin-bottom: 1rem;
      align-items: center;
      flex-wrap: wrap;
    }

    .bar {
      height: 20px;
      background: #eee;
      border-radius: 4px;
      overflow: hidden;
      margin-top: 0.75rem;
    }

    .bar > div {
      height: 100%;
      background: #4caf50;
      width: 0%;
      transition: width 0.3s;
    }

    .task {
      border: 1px solid #ddd;
      padding: 1rem;
      margin-bottom: 1rem;
      border-radius: 6px;
    }

    code {
      background: #f4f4f4;
      padding: 1px 4px;
      border-radius: 3px;
    }
  </style>
</head>

<body>
  <h1>Real-Time Celery + WebSocket</h1>

  <div class="row">
    <label>
      Total steps
      <input
        id="steps"
        type="number"
        value="10"
        min="1"
        max="50"
      />
    </label>

    <label>
      Delay (s)
      <input
        id="delay"
        type="number"
        value="1"
        step="0.1"
        min="0.1"
        max="5"
      />
    </label>

    <button id="submit">Submit new task</button>
  </div>

  <div id="tasks"></div>

  <script>
    const tasksEl = document.getElementById("tasks");

    document.getElementById("submit").onclick = async () => {
      const steps = document.getElementById("steps").value;
      const delay = document.getElementById("delay").value;

      const response = await fetch(
        `/submit-task?total_steps=${steps}&step_delay=${delay}`,
        {
          method: "POST",
        }
      );

      const { task_id } = await response.json();

      const el = document.createElement("div");
      el.className = "task";

      el.innerHTML = `
        <div>
          <strong>Task:</strong>
          <code>${task_id}</code>
        </div>

        <div class="bar">
          <div></div>
        </div>

        <div class="status">Connecting...</div>
      `;

      tasksEl.prepend(el);

      const bar = el.querySelector(".bar > div");
      const status = el.querySelector(".status");

      // WebSocket requires ws:// or wss://, not http:// or https://.
      const protocol = location.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(
        `${protocol}//${location.host}/ws/${task_id}`
      );

      ws.onopen = () => {
        status.textContent = "Connected — waiting for progress...";
      };

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        const percent = data.percent ?? 0;

        bar.style.width = `${percent}%`;

        status.textContent =
          `${data.state} — ` +
          `${data.current ?? 0}/${data.total ?? 0} ` +
          `(${percent}%)`;

        if (
          data.state === "SUCCESS" ||
          data.state === "FAILURE"
        ) {
          ws.close();
        }
      };

      ws.onerror = () => {
        status.textContent = "WebSocket connection error";
      };

      ws.onclose = () => {
        if (!status.textContent.startsWith("SUCCESS") &&
            !status.textContent.startsWith("FAILURE")) {
          status.textContent += " — connection closed";
        }
      };
    };
  </script>
</body>
</html>


### Important Fix

The browser must use:

```javascript
ws://
```

for HTTP or:

```javascript
wss://
```

for HTTPS.

Using `location.origin` directly with `new WebSocket()` would produce an invalid WebSocket URL such as:

```text
http://localhost:8000/ws/<task_id>
```

The corrected implementation converts the protocol automatically.

---

# Task 07 — Create the Dockerfile

Open `Dockerfile`:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000
EXPOSE 5555

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

The Docker image contains all Python dependencies and application files.


# Task 08 — Create Docker Compose Configuration

Open `docker-compose.yml`:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  web:
    build: .
    command: uvicorn main:app --host 0.0.0.0 --port 8000
    ports:
      - "8000:8000"

    environment:
      REDIS_HOST: redis
      REDIS_PORT: 6379

    depends_on:
      redis:
        condition: service_healthy

  worker:
    build: .
    command: celery -A celery_app worker --loglevel=info

    environment:
      REDIS_HOST: redis
      REDIS_PORT: 6379

    depends_on:
      redis:
        condition: service_healthy

  flower:
    build: .
    command: celery -A celery_app flower --port=5555
    ports:
      - "5555:5555"

    environment:
      REDIS_HOST: redis
      REDIS_PORT: 6379

    depends_on:
      - redis
      - worker
```

### Service Responsibilities

| Service | Responsibility | Port |
|---|---|---|
| `redis` | Broker, backend, Pub/Sub | 6379 |
| `web` | FastAPI + WebSocket | 8000 |
| `worker` | Executes Celery tasks | - |
| `flower` | Celery monitoring | 5555 |



# Task 09 — Start the Application

Build and start all services:

```bash
docker compose up --build
```

You should see logs from:

```text
redis
web
worker
flower
```

Keep this terminal running.

To run everything in the background instead:

```bash
docker compose up --build -d
```

Check the services:

```bash
docker compose ps
```

All four services should be running.

---

# Task 10 — Test the Web Application

Open:

```text
http://localhost:8000/static/index.html
```

On a Poridhi VM, use the URL provided by the lab's Load Balancer / URL panel for port `8000`.

You should see:

```text
Real-Time Celery + WebSocket

Total steps [10]
Delay (s) [1]

[Submit new task]
```


# Task 11 — Submit a Task

Set:

```text
Total steps: 10
Delay: 1
```

Click:

```text
Submit new task
```

A task should appear with a progress bar.

The progress should change approximately like:

```text
STARTED — 0/10 (0%)
PROGRESS — 1/10 (10%)
PROGRESS — 2/10 (20%)
PROGRESS — 3/10 (30%)
...
PROGRESS — 10/10 (100%)
SUCCESS — 10/10 (100%)
```

The progress is delivered through WebSocket instead of repeated HTTP polling.



# Task 12 — Test Multiple Tasks

Submit several tasks with different values.

For example:

```text
Task A
Steps: 10
Delay: 1 second
```

```text
Task B
Steps: 20
Delay: 0.5 seconds
```

```text
Task C
Steps: 5
Delay: 2 seconds
```

Each task should have its own progress bar.

This demonstrates why the task-specific Redis channel is important:

```text
task-progress:<task_id>
```


# Task 13 — Check Task Status Through HTTP

The WebSocket provides real-time updates, but the API also provides a traditional status endpoint.

Use:

```text
GET /task-status/{task_id}
```

For example:

```bash
curl http://localhost:8000/task-status/YOUR_TASK_ID
```

A completed task may return:

```json
{
  "task_id": "YOUR_TASK_ID",
  "state": "SUCCESS",
  "result": {
    "message": "Task YOUR_TASK_ID completed",
    "steps_completed": 10
  }
}
```

This endpoint is useful as a fallback when a client cannot maintain a WebSocket connection.


# Task 14 — Open Flower

Open:

```text
http://localhost:5555
```

On a Poridhi VM, expose port `5555` using the Load Balancer / URL panel and open the generated URL.

Flower should display information about:

- Celery workers
- Tasks
- Task states
- Task execution
- Worker activity

Submit another task and observe it in Flower.


# Task 15 — Inspect Docker Compose

Run:

```bash
docker compose ps
```

You should see four running services:

```text
redis
web
worker
flower
```

You can also inspect logs.

Web logs:

```bash
docker compose logs web
```

Worker logs:

```bash
docker compose logs worker
```

Redis logs:

```bash
docker compose logs redis
```

Flower logs:

```bash
docker compose logs flower
```


# Task 16 — Verify Redis Databases

Open a Redis shell:

```bash
docker compose exec redis redis-cli
```

Check Redis:

```text
PING
```

Expected:

```text
PONG
```

Check DB 0:

```text
SELECT 0
DBSIZE
```

DB 0 is used by Celery as the broker.

Check DB 1:

```text
SELECT 1
DBSIZE
```

DB 1 is used by Celery as the result backend.

DB 2 is used for Pub/Sub channels:

```text
SELECT 2
PUBSUB CHANNELS
```

A channel may appear while a WebSocket client is connected:

```text
task-progress:<task_id>
```

Exit Redis:

```text
QUIT


# Task 17 — Understand the Complete Flow

When the user clicks **Submit new task**, the following process occurs:

```text
Browser
   │
   │ POST /submit-task
   ▼
FastAPI
   │
   │ process_task.delay()
   ▼
Redis DB 0
   │
   │ task message
   ▼
Celery Worker
   │
   ├── update Celery state → Redis DB 1
   │
   └── publish progress → Redis DB 2
                              │
                              ▼
                         FastAPI WebSocket
                              │
                              ▼
                           Browser


Flower independently monitors the Celery worker and task events.


# Conclusion

In this lab, you built a real-time background task processing system using FastAPI, Celery, Redis, WebSockets, Docker Compose, and Flower. Celery handles background tasks, Redis manages messaging and task results, FastAPI provides REST/WebSocket communication, and Flower enables monitoring.



