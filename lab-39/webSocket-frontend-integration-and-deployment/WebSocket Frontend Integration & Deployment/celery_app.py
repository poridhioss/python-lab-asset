"""Celery application bootstrap.

Reused from the prior Celery lab. Worker pulls jobs from Redis DB 0
and we additionally use Redis as the pub/sub channel for live updates.
"""

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

# Auto-discover tasks defined in tasks.py
celery_app.autodiscover_tasks(["tasks"])
