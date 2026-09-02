#!/usr/bin/env bash
# Start Redis server (broker + pub/sub channel).
set -euo pipefail

if ! command -v redis-server >/dev/null 2>&1; then
    echo "redis-server not installed; run: sudo apt install -y redis-server" >&2
    exit 1
fi

if redis-cli ping >/dev/null 2>&1; then
    echo "redis already running"
    exit 0
fi

sudo systemctl enable --now redis-server
redis-cli ping
