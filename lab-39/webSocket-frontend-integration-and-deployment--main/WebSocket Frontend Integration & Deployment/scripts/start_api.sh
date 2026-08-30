#!/usr/bin/env bash
# Start the Flask API (serves /tasks and static/index.html).
set -euo pipefail
cd "$(dirname "$0")/.."
source venv/bin/activate

exec gunicorn \
    --bind 0.0.0.0:5000 \
    --workers 1 \
    --threads 4 \
    --access-logfile - \
    --error-logfile - \
    app:app
