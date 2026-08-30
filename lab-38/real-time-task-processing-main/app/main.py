import asyncio
import json
import logging
import os
import uuid

import redis.asyncio as aioredis
from fastapi import FastAPI, File, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from .tasks import resize_image

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PUBSUB_DB = int(os.getenv("REDIS_PUBSUB_DB", "1"))

MAX_UPLOAD_BYTES = int(os.getenv("MAX_UPLOAD_BYTES", str(25 * 1024 * 1024)))
OUTPUT_ROOT = os.getenv("LAB38_OUTPUT_DIR", "/tmp/lab38")

app = FastAPI(title="Lab 38 - Real-Time Task Progress")

# Permissive CORS — fine for a teaching lab; tighten for production.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def home():
    return FileResponse("static/index.html")


@app.get("/healthz")
async def healthz():
    """Liveness probe — handy for Docker / k8s."""
    return {"status": "ok"}


@app.post("/tasks")
async def create_task(file: UploadFile = File(...)):
    task_id = str(uuid.uuid4())

    output_dir = os.path.join(OUTPUT_ROOT, task_id)
    os.makedirs(output_dir, exist_ok=True)

    # Preserve the original extension so Pillow can infer the format.
    _, ext = os.path.splitext(file.filename or "image.bin")
    if not ext:
        ext = ".bin"

    input_path = os.path.join(output_dir, f"original{ext}")

    contents = await file.read()
    if len(contents) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds {MAX_UPLOAD_BYTES} bytes",
        )

    with open(input_path, "wb") as out:
        out.write(contents)

    logger.info("queued task %s for %s (%d bytes)", task_id, file.filename, len(contents))

    # Dispatch immediately — the WebSocket client is responsible for
    # subscribing before publishing begins (see /ws/{task_id}).
    resize_image.delay(task_id, input_path)

    return {
        "task_id": task_id,
        "status": "queued",
        "websocket": f"/ws/{task_id}",
    }


@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await websocket.accept()

    redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_PUBSUB_DB,
        decode_responses=True,
    )
    pubsub = redis_client.pubsub()
    channel = f"task_progress:{task_id}"

    await pubsub.subscribe(channel)

    # Signal readiness so the client knows events may now arrive.
    await websocket.send_json({
        "task_id": task_id,
        "status": "subscribed",
        "stage": "waiting",
        "label": "Subscribed to task channel",
        "progress": 0,
        "step": 0,
        "total": 100,
    })

    try:
        # pubsub.listen() returns an async iterator — this is the
        # correct async equivalent of get_message() and does NOT
        # block the event loop.
        async with pubsub.listen() as listener:
            async for raw in listener:
                if raw is None:
                    await asyncio.sleep(0.05)
                    continue
                if raw.get("type") != "message":
                    # Ignore 'subscribe' confirmations and similar.
                    continue

                try:
                    data = json.loads(raw["data"])
                except (TypeError, ValueError):
                    logger.warning("dropping malformed pubsub message: %r", raw)
                    continue

                await websocket.send_json(data)

                if data.get("status") in ("completed", "failed"):
                    break

    except WebSocketDisconnect:
        logger.info("client disconnected for task %s", task_id)
    except Exception:
        logger.exception("websocket error for task %s", task_id)
    finally:
        try:
            await pubsub.unsubscribe(channel)
        except Exception:  # pragma: no cover
            pass
        await pubsub.close()
        await redis_client.close()


@app.get("/files/{task_id}/{name}")
async def get_output_file(task_id: str, name: str):
    # Restrict the filename to safe characters to avoid path traversal.
    safe_name = os.path.basename(name)
    if safe_name != name or not safe_name:
        raise HTTPException(status_code=400, detail="Invalid filename")

    file_path = os.path.join(OUTPUT_ROOT, task_id, safe_name)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    return FileResponse(file_path, filename=safe_name)