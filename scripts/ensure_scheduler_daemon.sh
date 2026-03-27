#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/wdsr88/workspace/y2i"
PYTHON_BIN="$ROOT/.venv/bin/python"
LOG_DIR="$ROOT/.omx/logs"
PID_FILE="$LOG_DIR/omx-scheduler-daemon.pid"
SIGNATURE="omx_brainstorm.cli run-scheduler --config config.toml"

mkdir -p "$LOG_DIR"
cd "$ROOT"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    existing_cmdline="$(tr '\0' ' ' </proc/"$existing_pid"/cmdline 2>/dev/null || true)"
    if [[ "$existing_cmdline" == *"$SIGNATURE"* ]]; then
      exit 0
    fi
  fi
fi

nohup setsid "$PYTHON_BIN" -m omx_brainstorm.cli run-scheduler --config config.toml </dev/null >>"$LOG_DIR/omx-scheduler-daemon.log" 2>&1 &
echo $! >"$PID_FILE"
