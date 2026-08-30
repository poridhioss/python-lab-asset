import os

from celery import Celery

# Broker/backend live on db=0, Pub/Sub events on db=1.
# Keeping them on separate logical databases avoids pubsub traffic
# interfering with the broker queue.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

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
    # Concurrency hint — useful when students run multiple tasks.
    worker_concurrency=2,
)