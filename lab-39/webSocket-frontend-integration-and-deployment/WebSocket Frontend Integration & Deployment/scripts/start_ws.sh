#!/usr/bin/env bash
# Start the WebSocket bridge (uvicorn + python-socketio).
set -euo pipefail
cd "$(dirname "$0")/.."
source venv/bin/activate

exec uvicorn ws_server:app --host 0.0.0.0 --port 5556 --log-level info
