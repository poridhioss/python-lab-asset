#!/usr/bin/env bash
# Stop every long-running process started by the start_*.sh scripts.
set -euo pipefail

pkill -f "celery -A celery_app.celery_app worker" || true
pkill -f "gunicorn .* app:app" || true
pkill -f "uvicorn ws_server:app" || true
echo "stopped"
