#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/wdsr88/workspace/y2i"
PYTHON_BIN="$ROOT/.venv/bin/python"
LOG_DIR="$ROOT/.omx/logs"
PID_PATTERN="$PYTHON_BIN -m omx_brainstorm.cli run-scheduler --config config.toml"

mkdir -p "$LOG_DIR"
cd "$ROOT"

if pgrep -f "$PID_PATTERN" >/dev/null 2>&1; then
  exit 0
fi

nohup setsid "$PYTHON_BIN" -m omx_brainstorm.cli run-scheduler --config config.toml </dev/null >>"$LOG_DIR/omx-scheduler-daemon.log" 2>&1 &
echo $! >"$LOG_DIR/omx-scheduler-daemon.pid"
