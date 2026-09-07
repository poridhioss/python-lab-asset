# Lab 38 — Real-Time Task Progress System

## Introduction

Modern web applications often need to show users the live progress of a long-running task — for example, resizing an uploaded image, transcoding a video, or generating a report. In this lab, you will build a real-time task progress system: a user uploads an image through the browser, a Celery worker resizes it into a thumbnail and a medium-sized version using Pillow, and FastAPI streams every progress event to the browser over a WebSocket using Redis Pub/Sub. 

## Objectives

- Accept an uploaded file in a FastAPI endpoint using `UploadFile`.
- Run real image processing work in a Celery worker using Pillow.
- Publish multi-stage task progress from Celery through Redis Pub/Sub.
- Build a WebSocket endpoint with FastAPI that streams progress events live.
- Combine multi-stage progress (thumbnail + medium) into a single overall percentage on the frontend.
- 

## 1. Architecture Overview

![Request Flow 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/requestflow_2.png)


## 2. Why Redis Pub/Sub?

Without real-time streaming, the frontend may repeatedly ask the server "are we done yet?":

```text
GET /task-status/<id>
GET /task-status/<id>
GET /task-status/<id>
GET /task-status/<id>
```

This is called **polling**. It wastes bandwidth, adds latency, and does not scale.

With Redis Pub/Sub, the worker pushes an event whenever progress changes:

```text
Celery Worker
     │
     │ 10%  thumbnail
     │ 30%  thumbnail
     │ 60%  thumbnail
     │ 100% thumbnail
     │ 50%  medium
     │ 100% medium
     │ completed
     ▼
Redis Pub/Sub
     │
     ▼
FastAPI WebSocket
     │
     ▼
Browser
```

The browser receives every progress update immediately and the connection stays idle the rest of the time. No polling, no wasted requests.

## 3. Components

| Component | Responsibility |
|---|---|
| FastAPI | HTTP file upload, WebSocket server, file download endpoint |
| Celery | Executes the image resize background task |
| Redis (db=0) | Celery broker and result backend |
| Redis (db=1) | Pub/Sub channel for progress events |
| Pillow | Image processing library used by the Celery task |
| Browser | Uploads file, shows live progress, downloads output |

## 4. Environment Setup

### Step 1 — Update the VM

```bash
sudo apt update -y
```
![Lab 38 Image 1](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image1.png)

Check Python:

```bash
python3 --version
```

### Step 2 — Install Redis

```bash
sudo apt install -y redis-server
```

![Lab 38 Image 2](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image2.png)

Start Redis:

```bash
sudo systemctl start redis-server
```
![Lab 38 Image 3](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image3.png)

Enable Redis on boot:

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
mkdir -p ~/lab38-real-time-progress
cd ~/lab38-real-time-progress
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
pip install "fastapi" "uvicorn[standard]" "celery" "redis>=4.2" "python-multipart" "Pillow"
```
![Lab 38 Image 4](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image4.png)

Create `requirements.txt`:

```bash
cat << 'EOF' > requirements.txt
fastapi
uvicorn[standard]
celery
redis>=4.2
python-multipart
Pillow
EOF
```

> **Note:** `python-multipart` is required by FastAPI for `UploadFile` and form parsing. `Pillow` is required by the `resize_image` task.

## 7. Project Structure

```text
lab38-real-time-progress/
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

Create the application directories:

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
    "lab38",
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
import os
import time

import redis
from PIL import Image

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


def publish(task_id: str, payload: dict) -> None:
    """Publish a JSON event to the task's progress channel."""
    channel = f"task_progress:{task_id}"
    redis_client.publish(channel, json.dumps(payload))


@celery_app.task(name="resize_image")
def resize_image(task_id: str, input_path: str) -> dict:
    """Resize an uploaded image into a thumbnail and a medium size.

    For each stage we publish a sequence of progress events so the
    frontend can update the progress bar and the current stage label
    in real time. On success we publish a final ``completed`` event
    that includes the download URLs for the output files.
    """

    output_dir = f"/tmp/lab38/{task_id}"
    os.makedirs(output_dir, exist_ok=True)

    thumbnail_path = os.path.join(output_dir, "thumbnail.jpg")
    medium_path = os.path.join(output_dir, "medium.jpg")

    try:
        image = Image.open(input_path)
        image.load()

        # ------------------------------------------------------------------
        # Stage 1: Thumbnail (max 128 x 128)
        # ------------------------------------------------------------------
        publish(task_id, {
            "task_id": task_id,
            "status": "processing",
            "stage": "thumbnail",
            "label": "Generating thumbnail",
            "progress": 0,
            "step": 0,
            "total": 100,
        })

        thumb = image.copy()
        thumb.thumbnail((128, 128))

        for pct in (20, 40, 60, 80, 100):
            time.sleep(1)

            publish(task_id, {
                "task_id": task_id,
                "status": "processing",
                "stage": "thumbnail",
                "label": "Generating thumbnail",
                "progress": pct,
                "step": pct,
                "total": 100,
            })

        thumb.save(thumbnail_path, format="JPEG", quality=85)

        # ------------------------------------------------------------------
        # Stage 2: Medium (max 512 x 512)
        # ------------------------------------------------------------------
        publish(task_id, {
            "task_id": task_id,
            "status": "processing",
            "stage": "medium",
            "label": "Generating medium size",
            "progress": 0,
            "step": 0,
            "total": 100,
        })

        medium = image.copy()
        medium.thumbnail((512, 512))

        for pct in (20, 40, 60, 80, 100):
            time.sleep(1)

            publish(task_id, {
                "task_id": task_id,
                "status": "processing",
                "stage": "medium",
                "label": "Generating medium size",
                "progress": pct,
                "step": pct,
                "total": 100,
            })

        medium.save(medium_path, format="JPEG", quality=85)

        # ------------------------------------------------------------------
        # Final: completed
        # ------------------------------------------------------------------
        event = {
            "task_id": task_id,
            "status": "completed",
            "stage": "done",
            "label": "Done",
            "progress": 100,
            "step": 100,
            "total": 100,
            "output_files": [
                {
                    "name": "thumbnail.jpg",
                    "url": f"/files/{task_id}/thumbnail.jpg",
                },
                {
                    "name": "medium.jpg",
                    "url": f"/files/{task_id}/medium.jpg",
                },
            ],
        }

        publish(task_id, event)

        return event

    except Exception as exc:  # pragma: no cover - defensive
        publish(task_id, {
            "task_id": task_id,
            "status": "failed",
            "stage": "error",
            "label": "Failed",
            "error": str(exc),
        })
        raise
EOF
```

## 10. Create FastAPI WebSocket Server

Create `app/main.py`:

```bash
cat << 'EOF' > app/main.py
import asyncio
import json
import os
import uuid

import redis.asyncio as redis
from fastapi import FastAPI, File, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

from .tasks import resize_image


app = FastAPI(
    title="Lab 38 - Real-Time Task Progress"
)


@app.get("/")
async def home():
    return FileResponse("static/index.html")


@app.post("/tasks")
async def create_task(file: UploadFile = File(...)):

    task_id = str(uuid.uuid4())

    output_dir = f"/tmp/lab38/{task_id}"
    os.makedirs(output_dir, exist_ok=True)

    # Preserve the original extension so Pillow can infer the format.
    _, ext = os.path.splitext(file.filename or "image.bin")
    if not ext:
        ext = ".bin"

    input_path = os.path.join(output_dir, f"original{ext}")

    contents = await file.read()
    with open(input_path, "wb") as out:
        out.write(contents)

    # Give the client a moment to open the WebSocket
    # before the worker publishes its first event.
    await asyncio.sleep(0.5)

    resize_image.delay(task_id, input_path)

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

                data = json.loads(message["data"])

                await websocket.send_json(data)

                if data.get("status") in ("completed", "failed"):
                    break

            await asyncio.sleep(0.05)

    except WebSocketDisconnect:
        pass

    finally:

        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.close()


@app.get("/files/{task_id}/{name}")
async def get_output_file(task_id: str, name: str):

    # Restrict the filename to safe characters to avoid path traversal.
    safe_name = os.path.basename(name)
    file_path = f"/tmp/lab38/{task_id}/{safe_name}"

    if not os.path.exists(file_path):
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=safe_name)
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
    max-width: 720px;
    margin: 50px auto;
    padding: 20px;
    color: #222;
}

h1 {
    margin-bottom: 10px;
}

.subtitle {
    color: #666;
    margin-top: 0;
    margin-bottom: 25px;
}

form {
    display: flex;
    gap: 10px;
    align-items: center;
    margin-bottom: 25px;
}

input[type="file"] {
    flex: 1;
}

button {
    padding: 10px 20px;
    font-size: 16px;
    cursor: pointer;
    background: #4caf50;
    color: white;
    border: none;
    border-radius: 4px;
}

button:disabled {
    background: #aaa;
    cursor: not-allowed;
}

.progress-container {
    width: 100%;
    height: 30px;
    background: #ddd;
    border-radius: 4px;
    overflow: hidden;
    margin-top: 10px;
}

.progress-bar {
    height: 100%;
    width: 0%;
    background: #4caf50;
    color: white;
    text-align: center;
    line-height: 30px;
    transition: width 0.25s ease-in-out;
}

#stage {
    margin-top: 12px;
    font-weight: bold;
}

#status {
    margin-top: 12px;
    padding: 12px;
    background: #f4f4f4;
    border-radius: 4px;
    white-space: pre-line;
    font-family: monospace;
    min-height: 60px;
}

#files {
    margin-top: 20px;
}

#files a {
    display: inline-block;
    margin-right: 10px;
    margin-top: 5px;
    padding: 8px 14px;
    background: #2196f3;
    color: white;
    text-decoration: none;
    border-radius: 4px;
}

#files a:hover {
    background: #1976d2;
}

.error {
    color: #c62828;
}

</style>

</head>

<body>

<h1>Real-Time Image Resize</h1>

<p class="subtitle">
    Upload an image. The Celery worker will resize it into a thumbnail
    and a medium-sized version while the page streams live progress.
</p>

<form id="upload-form">

    <input
        type="file"
        id="file-input"
        accept="image/*"
        required
    >

    <button type="submit" id="upload-button">
        Upload &amp; Resize
    </button>

</form>

<div class="progress-container">

    <div id="progress" class="progress-bar">
        0%
    </div>

</div>

<div id="stage">Waiting for task...</div>

<div id="status">No task running.</div>

<div id="files"></div>

<script>

const STAGE_WEIGHT = {
    thumbnail: { start: 0,   end: 50  },
    medium:    { start: 50,  end: 100 },
    done:      { start: 100, end: 100 },
};

function overallPercent(data) {
    if (data.status === "completed") return 100;
    if (data.status === "failed") return 0;

    const range = STAGE_WEIGHT[data.stage] || { start: 0, end: 0 };
    const slice = range.end - range.start;
    return Math.round(range.start + (data.progress / 100) * slice);
}

async function startTask(file) {

    const statusEl = document.getElementById("status");
    const stageEl = document.getElementById("stage");
    const progressEl = document.getElementById("progress");
    const filesEl = document.getElementById("files");
    const button = document.getElementById("upload-button");

    filesEl.innerHTML = "";
    button.disabled = true;

    stageEl.textContent = "Uploading...";
    statusEl.textContent = "Sending file to the server...";

    const formData = new FormData();
    formData.append("file", file);

    const response = await fetch("/tasks", {
        method: "POST",
        body: formData,
    });

    if (!response.ok) {
        statusEl.textContent = `Upload failed: ${response.status}`;
        stageEl.textContent = "Error";
        stageEl.classList.add("error");
        button.disabled = false;
        return;
    }

    const task = await response.json();

    stageEl.classList.remove("error");
    stageEl.textContent = "Waiting for events...";
    statusEl.textContent =
        `Task ID: ${task.task_id}\nStatus: ${task.status}`;

    const protocol =
        window.location.protocol === "https:" ? "wss" : "ws";

    const socket = new WebSocket(
        `${protocol}://${window.location.host}${task.websocket}`
    );

    socket.onmessage = function (event) {

        const data = JSON.parse(event.data);

        const overall = overallPercent(data);
        progressEl.style.width = `${overall}%`;
        progressEl.textContent = `${overall}%`;

        stageEl.textContent =
            data.label || data.stage || data.status;

        statusEl.textContent =
            `Task ID: ${data.task_id}\n` +
            `Status:  ${data.status}\n` +
            `Stage:   ${data.stage || "-"}\n` +
            `Step:    ${data.step}/${data.total}\n` +
            `Progress: ${data.progress}%`;

        if (data.status === "completed") {

            stageEl.textContent = "Completed";

            if (Array.isArray(data.output_files)) {
                for (const file of data.output_files) {
                    const link = document.createElement("a");
                    link.href = file.url;
                    link.download = file.name;
                    link.textContent = `Download ${file.name}`;
                    filesEl.appendChild(link);
                }
            }

            socket.close();
            button.disabled = false;

        } else if (data.status === "failed") {

            stageEl.textContent = `Failed: ${data.error || "unknown error"}`;
            stageEl.classList.add("error");
            socket.close();
            button.disabled = false;

        }

    };

    socket.onerror = function () {

        statusEl.textContent += "\nWebSocket connection error.";
        button.disabled = false;

    };

    socket.onclose = function () {

        button.disabled = false;

    };
}

document.getElementById("upload-form").addEventListener(
    "submit",
    function (event) {
        event.preventDefault();
        const input = document.getElementById("file-input");
        if (input.files && input.files.length > 0) {
            startTask(input.files[0]);
        }
    }
);

</script>

</body>

</html>
EOF
```

## 12. Start Celery Worker

Open **Terminal 1**:

```bash
cd ~/lab38-real-time-progress
source venv/bin/activate
```

Start the worker:

```bash
celery -A app.celery_app worker --loglevel=info
```
![Lab 38 Image 5](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image5.png)
Expected output should contain:

```text
[tasks]
  . resize_image
```

and:

```text
celery@... ready.
```

## 13. Start FastAPI

Open **Terminal 2**:

```bash
cd ~/lab38-real-time-progress
source venv/bin/activate
```

Run:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```
![Lab 38 Image 6](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image6.png)

Expected:

```text
Uvicorn running on http://0.0.0.0:8000
```

## 14. Test the API

Open **Terminal 3**.

You can upload an image directly with `curl` and watch the worker process it:

```bash
curl -X POST http://127.0.0.1:8000/tasks \
    -F "file=@/path/to/your-image.jpg"
```

Expected:

![Lab 38 Image 8](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image8.png)

```json
{
    "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "status": "queued",
    "websocket": "/ws/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

You can then download one of the outputs:

```bash
curl -O -J http://127.0.0.1:8000/files/<TASK_ID>/thumbnail.jpg
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
Real-Time Image Resize

![Lab 38 Image 7](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image7.png)

[ Choose File ] [ Upload & Resize ]

0%
Waiting for task...
```

Pick any image file (`.jpg`, `.png`, `.webp` …) and click **Upload & Resize**.

The progress bar will fill in two stages:

```text
Stage 1: Generating thumbnail   (0% → 100%, mapped to overall 0% → 50%)
Stage 2: Generating medium size (0% → 100%, mapped to overall 50% → 100%)
```

Overall progress advances like this:

```text
0% → 10% → 20% → 30% → 40% → 50% → 60% → 70% → 80% → 90% → 100%
```

When the task finishes, two download links appear:

```text
[ Download thumbnail.jpg ]   [ Download medium.jpg ]
```

Clicking either link downloads the resized image. The page does not need to be refreshed — the browser receives every update live over the WebSocket.

## 16. Verify Redis Pub/Sub

Open **Terminal 4**:

```bash
redis-cli
```

Subscribe to the task channel (paste a `task_id` from step 14):

```text
SUBSCRIBE task_progress:<TASK_ID>
```

You will receive events like:

```json
{
    "task_id": "...",
    "status": "processing",
    "stage": "thumbnail",
    "label": "Generating thumbnail",
    "progress": 40,
    "step": 40,
    "total": 100
}
```

Final event:

```json
{
    "task_id": "...",
    "status": "completed",
    "stage": "done",
    "label": "Done",
    "progress": 100,
    "step": 100,
    "total": 100,
    "output_files": [
        { "name": "thumbnail.jpg", "url": "/files/.../thumbnail.jpg" },
        { "name": "medium.jpg",    "url": "/files/.../medium.jpg"    }
    ]
}
```

## 17. Verify Celery Worker

The Celery terminal should show:

```text
Task resize_image[...] received
```

and after completion:

```text
Task resize_image[...] succeeded in 11.234s
```


## 18. Real-Time Frontend 

The lab implementation uses the **browser's native WebSocket API** to connect directly to the FastAPI WebSocket endpoint. This is the simplest path and works well for learning.


### Socket.IO (production consideration)

For production-grade frontends, **Socket.IO** is a common choice because it adds features that raw WebSockets don't provide out of the box:

- Automatic reconnection on dropped connections
- Rooms and namespaces for multi-tenant streaming
- HTTP long-polling fallback for restrictive networks
- Built-in acknowledgement / event semantics



The core architecture — **Celery → Redis Pub/Sub → server → browser** — stays the same; only the wire protocol between browser and server changes.



### Pillow fails to open the uploaded file

If the worker publishes a `failed` event with an error like `cannot identify image file`, the uploaded bytes were probably written to disk with the wrong extension. Make sure `app/main.py` preserves the original extension from `file.filename` and that `Image.open(input_path)` runs against the saved path.

### Permission denied when writing to /tmp/lab38

`os.makedirs(..., exist_ok=True)` should handle this. If you still hit a permission error, point the output dir at a location you own:

```python
output_dir = os.path.expanduser(f"~/lab38-output/{task_id}")
```

and update `app/tasks.py` and `app/main.py` to use the same path. Also update the `GET /files/{task_id}/{name}` route accordingly.

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

> **Tip:** If pubsub messages aren't reaching the WebSocket, double-check both `redis_client` instances (in `tasks.py` and `main.py`) use `db=1` while Celery's broker uses `db=0`.

## 20. Expected Output

### Celery Worker

```text
[tasks]
  . resize_image

celery@... ready.
```

After uploading an image:

```text
[INFO] Task app.tasks.resize_image[abc123] received
[INFO] Task app.tasks.resize_image[abc123] succeeded in 11.234s
```

### FastAPI

```text
Uvicorn running on http://0.0.0.0:8000
```

### Task Creation


![Lab 38 Image 9](https://raw.githubusercontent.com/poridhioss/python-lab-asset/9ae8cec6a817804f09867e6866d01218d203d9a3/lab38image9.png)

```json
{
    "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "status": "queued",
    "websocket": "/ws/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
}
```

### Real-Time Progress (browser)

```text
Stage: Generating thumbnail       Overall: 0% → 10% → 20% → 30% → 40% → 50%
Stage: Generating medium size     Overall: 50% → 60% → 70% → 80% → 90% → 100%
Stage: Completed                  Overall: 100%
```

### Final Event

```json
{
    "task_id": "...",
    "status": "completed",
    "stage": "done",
    "label": "Done",
    "progress": 100,
    "step": 100,
    "total": 100,
    "output_files": [
        { "name": "thumbnail.jpg", "url": "/files/.../thumbnail.jpg" },
        { "name": "medium.jpg",    "url": "/files/.../medium.jpg"    }
    ]
}
```

### Download Links (browser)

```text
[ Download thumbnail.jpg ]   [ Download medium.jpg ]
```


## Conclusion

In this lab, you built a real-time task progress system using **Celery, Redis Pub/Sub, FastAPI WebSocket, Pillow, and a browser-based native WebSocket client**. The user uploads an image, a Celery worker resizes it in two stages while publishing progress events, and FastAPI streams those events to the browser — culminating in download links for the resized output.
