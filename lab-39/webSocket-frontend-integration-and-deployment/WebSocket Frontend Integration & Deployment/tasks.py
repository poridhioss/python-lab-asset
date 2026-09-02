"""Celery tasks that publish lifecycle events to Redis pub/sub.

Each task publishes JSON-encoded state messages to the per-task channel
``task:<task_id>``. The WebSocket server (ws_server.py) subscribes to those
channels and forwards the messages to the right Socket.IO room so the
browser sees live updates.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime, timezone

import redis
from celery.utils.log import get_task_logger

from celery_app import celery_app

logger = get_task_logger(__name__)

REDIS_URL = "redis://127.0.0.1:6379/0"
_redis = redis.Redis.from_url(REDIS_URL, decode_responses=True)


def publish_state(task_id: str, state: str, **extra) -> None:
    """Publish a JSON state message on the per-task Redis channel.

    Channel: ``task:<task_id>``
    Payload keys: ``state``, ``task_id``, ``timestamp``, plus any extras
    such as ``result``, ``error``, ``progress``.
    """
    payload = {
        "state": state,
        "task_id": task_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **extra,
    }
    try:
        _redis.publish(f"task:{task_id}", json.dumps(payload))
    except redis.RedisError as exc:  # pragma: no cover - logging only
        logger.warning("redis publish failed: %s", exc)
    logger.info("published %s for %s", state, task_id)


@celery_app.task(
    bind=True,
    name="tasks.process_order",
    autoretry_for=(RuntimeError,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3},
)
def process_order(self, payload: str, fail_probability: float = 0.0) -> dict:
    """Simulate processing an order with optional simulated failures.

    Publishes STARTED, PROGRESS, SUCCESS / FAILURE / RETRY events.
    """
    task_id = self.request.id
    publish_state(task_id, "STARTED", payload=payload)

    for step in range(1, 6):
        time.sleep(0.5)
        publish_state(
            task_id,
            "PROGRESS",
            payload=payload,
            progress=step,
            total=5,
            message=f"step {step}/5 complete",
        )

    if random.random() < fail_probability:
        publish_state(task_id, "FAILURE", error="simulated failure")
        raise RuntimeError("simulated failure")

    result = {"payload": payload, "status": "completed", "by": "celery"}
    publish_state(task_id, "SUCCESS", result=result)
    return result