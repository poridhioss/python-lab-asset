#!/usr/bin/env bash
# Start the Celery worker.
set -euo pipefail
cd "$(dirname "$0")/.."
source venv/bin/activate

exec celery -A celery_app.celery_app worker --loglevel=info --concurrency=2 -E
