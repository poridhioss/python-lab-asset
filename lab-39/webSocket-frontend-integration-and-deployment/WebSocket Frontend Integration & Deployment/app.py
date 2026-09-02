"""Flask API.

* ``POST /tasks`` -- enqueue a Celery task and return its ``task_id``.
* ``GET /`` -- serve the static frontend (static/index.html).
"""

from __future__ import annotations

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
    async_result = process_order.delay(payload, fail_probability)
    return jsonify(task_id=async_result.id, payload=payload, fail_probability=fail_probability), 202


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("API_PORT", "5000")))