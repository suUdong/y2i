#!/usr/bin/env bash
# Cron-friendly heartbeat for the y2i scheduler.
#
# Each invocation:
#   1. Ensures the run-scheduler daemon is running (idempotent — restarts if
#      the previous process died).
#   2. Runs run-healthcheck with a stale-threshold gate. If the scheduler's
#      last successful run is older than the threshold, the healthcheck
#      exits non-zero and we log a stale alert. Cron's MAILTO (or piped
#      notifier) can pick that up.
#
# Designed for a `*/10 * * * *` cron entry. Idempotent and self-healing —
# safe to run while a daemon is already up.
set -uo pipefail

ROOT="${Y2I_ROOT:-/home/wdsr88/workspace/y2i}"
PYTHON_BIN="${Y2I_PYTHON:-$ROOT/.venv/bin/python}"
STALE_HOURS="${Y2I_STALE_HOURS:-6}"
LOG_DIR="$ROOT/.omx/logs"
LOG_FILE="$LOG_DIR/scheduler_heartbeat.log"

mkdir -p "$LOG_DIR"

log() {
  printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >> "$LOG_FILE"
}

log "heartbeat tick (stale_hours=$STALE_HOURS)"

# 1. Make sure the daemon is up. ensure_scheduler_daemon.sh is idempotent.
"$ROOT/scripts/ensure_scheduler_daemon.sh" >> "$LOG_FILE" 2>&1
daemon_rc=$?
if [[ $daemon_rc -ne 0 ]]; then
  log "ensure_scheduler_daemon failed (rc=$daemon_rc)"
fi

# 2. Stale-detection healthcheck. Non-zero exit means stale → cron alerts.
if [[ -x "$PYTHON_BIN" ]]; then
  "$PYTHON_BIN" -m omx_brainstorm.cli run-healthcheck \
      --stale-threshold-hours "$STALE_HOURS" \
      --exit-nonzero-on-stale >> "$LOG_FILE" 2>&1
  health_rc=$?
  if [[ $health_rc -eq 2 ]]; then
    log "ALERT: scheduler is stale beyond ${STALE_HOURS}h threshold"
    echo "y2i scheduler stale beyond ${STALE_HOURS}h — see $LOG_FILE" 1>&2
    exit 2
  elif [[ $health_rc -ne 0 ]]; then
    log "run-healthcheck failed (rc=$health_rc)"
  else
    log "healthcheck ok"
  fi
else
  log "python binary missing: $PYTHON_BIN"
  exit 3
fi
