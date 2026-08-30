import json
import logging
import os
import time

import redis
from PIL import Image

from .celery_app import celery_app

logger = logging.getLogger(__name__)

# Pub/Sub messages live on db=1 to keep them out of the broker queue (db=0).
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PUBSUB_DB = int(os.getenv("REDIS_PUBSUB_DB", "1"))

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_PUBSUB_DB,
    decode_responses=True,
)


def publish(task_id: str, payload: dict) -> None:
    """Publish a JSON event to the task's progress channel."""
    channel = f"task_progress:{task_id}"
    redis_client.publish(channel, json.dumps(payload))
    logger.info("publish %s -> %s", channel, payload.get("status"))


@celery_app.task(name="resize_image", bind=True)
def resize_image(self, task_id: str, input_path: str) -> dict:
    """Resize an uploaded image into a thumbnail and a medium size.

    For each stage we publish a sequence of progress events so the
    frontend can update the progress bar and the current stage label
    in real time. On success we publish a final ``completed`` event
    that includes the download URLs for the output files.
    """
    # Allow output_dir to be overridden for environments without /tmp
    # write access.
    output_root = os.getenv("LAB38_OUTPUT_DIR", "/tmp/lab38")
    output_dir = os.path.join(output_root, task_id)
    os.makedirs(output_dir, exist_ok=True)

    thumbnail_path = os.path.join(output_dir, "thumbnail.jpg")
    medium_path = os.path.join(output_dir, "medium.jpg")

    try:
        # Use a context manager so the underlying file handle is closed
        # even on partial reads.
        with Image.open(input_path) as image:
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

            # Let Pillow infer the format from the .jpg extension so
            # PNG uploads with transparency are converted cleanly.
            thumb.convert("RGB").save(thumbnail_path, format="JPEG", quality=85)

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

            medium.convert("RGB").save(medium_path, format="JPEG", quality=85)

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

    except Exception as exc:
        logger.exception("resize_image failed for task %s", task_id)
        publish(task_id, {
            "task_id": task_id,
            "status": "failed",
            "stage": "error",
            "label": "Failed",
            "error": str(exc),
        })
        # Re-raise so Celery records the failure in the result backend.
        raise
