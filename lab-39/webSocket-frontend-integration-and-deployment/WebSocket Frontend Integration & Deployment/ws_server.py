"""WebSocket bridge: Redis pub/sub -> Socket.IO rooms.

* Each browser client emits ``subscribe_task`` with a ``task_id``.
* The server joins the Socket.IO room ``task_<task_id>`` and (if first
  subscriber) starts a background asyncio task that subscribes to the
  Redis channel ``task:<task_id>``.
* Each message received from Redis is forwarded to the room as a
  ``task_update`` event.
* When the last subscriber leaves the room, the Redis subscriber is
  cancelled to avoid leaking background tasks.

Run with::

    uvicorn ws_server:app --host 0.0.0.0 --port 5556
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from contextlib import suppress

import redis.asyncio as redis_async
import socketio

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
WS_PORT = int(os.environ.get("WS_PORT", "5556"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ws_server")

sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins="*",
    logger=False,
    engineio_logger=False,
)
app = socketio.ASGIApp(sio)

_redis = redis_async.from_url(REDIS_URL, decode_responses=True)

# task_id -> asyncio.Task running the redis subscriber
_subscribers: dict[str, asyncio.Task] = {}
# task_id -> current subscriber count
_refcounts: dict[str, int] = {}
# per-task asyncio lock to serialise refcount changes
_locks: dict[str, asyncio.Lock] = {}


def _lock_for(task_id: str) -> asyncio.Lock:
    if task_id not in _locks:
        _locks[task_id] = asyncio.Lock()
    return _locks[task_id]


async def _pump(task_id: str) -> None:
    """Subscribe to ``task:<task_id>`` and forward each message to the room."""
    channel = f"task:{task_id}"
    room = _room(task_id)
    pubsub = _redis.pubsub()
    try:
        await pubsub.subscribe(channel)
        log.info("subscribed to %s", channel)
        async for message in pubsub.listen():
            if message is None or message.get("type") != "message":
                continue
            data = message.get("data")
            try:
                payload = json.loads(data)
            except (TypeError, ValueError):
                payload = {"raw": data}
            await sio.emit("task_update", payload, room=room)
    except asyncio.CancelledError:
        log.info("pump cancelled for %s", channel)
        raise
    except Exception:  # pragma: no cover
        log.exception("pump error for %s", channel)
    finally:
        with suppress(Exception):
            await pubsub.unsubscribe(channel)
            await pubsub.close()


def _room(task_id: str) -> str:
    return f"task_{task_id}"


async def _acquire(task_id: str, sid: str) -> None:
    async with _lock_for(task_id):
        _refcounts[task_id] = _refcounts.get(task_id, 0) + 1
        await sio.enter_room(sid, _room(task_id))
        if task_id not in _subscribers or _subscribers[task_id].done():
            _subscribers[task_id] = asyncio.create_task(_pump(task_id))
            log.info("started subscriber for %s", task_id)


async def _release(task_id: str, sid: str) -> None:
    async with _lock_for(task_id):
        await sio.leave_room(sid, _room(task_id))
        _refcounts[task_id] = max(0, _refcounts.get(task_id, 0) - 1)
        if _refcounts[task_id] == 0 and task_id in _subscribers:
            _subscribers[task_id].cancel()
            with suppress(asyncio.CancelledError):
                await _subscribers[task_id]
            _subscribers.pop(task_id, None)
            _refcounts.pop(task_id, None)
            log.info("stopped subscriber for %s", task_id)


@sio.event
async def connect(sid, environ, auth):
    log.info("client connected sid=%s", sid)


@sio.event
async def disconnect(sid):
    # Best-effort cleanup: leave any room for this sid.
    # _release decrements refcounts if the sid is still tracked.
    for task_id in list(_refcounts.keys()):
        await _release(task_id, sid)


@sio.on("subscribe_task")
async def on_subscribe(sid, data):
    task_id = (data or {}).get("task_id")
    if not task_id:
        await sio.emit("task_update", {"state": "ERROR", "error": "task_id required"}, to=sid)
        return
    await _acquire(task_id, sid)
    await sio.emit("task_update", {"state": "SUBSCRIBED", "task_id": task_id}, to=sid)


@sio.on("unsubscribe_task")
async def on_unsubscribe(sid, data):
    task_id = (data or {}).get("task_id")
    if task_id:
        await _release(task_id, sid)


@app.on_event("shutdown")
async def _shutdown():  # pragma: no cover
    for t in list(_subscribers.values()):
        t.cancel()
    await _redis.aclose()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("ws_server:app", host="0.0.0.0", port=WS_PORT)
