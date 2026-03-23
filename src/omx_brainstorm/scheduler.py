from __future__ import annotations

import logging
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from .app_config import AppConfig
from .notifications import notify_all
from .utils import write_json, read_json

logger = logging.getLogger(__name__)
HEALTH_PATH = Path(".omx/state/scheduler_health.json")


def run_scheduled_job(config: AppConfig, script_path: str = "scripts/run_channel_30d_comparison.py") -> int:
    logger.info("Running scheduled comparison job")
    state = read_json(HEALTH_PATH, {"error_count": 0})
    proc = subprocess.run([sys.executable, script_path], text=True, capture_output=True, check=False)
    state["last_run_at"] = datetime.now(timezone.utc).isoformat()
    state["last_exit_code"] = proc.returncode
    if proc.returncode == 0:
        state["last_success_at"] = state["last_run_at"]
        state["status"] = "ok"
        notify_all(config.notifications, proc.stdout.strip() or "Scheduled job completed")
    else:
        state["last_error_at"] = state["last_run_at"]
        state["status"] = "error"
        state["error_count"] = int(state.get("error_count", 0)) + 1
        logger.error("Scheduled job failed: %s", proc.stderr.strip() or proc.stdout.strip())
        notify_all(config.notifications, "Scheduled job failed. Check server logs for details.")
    write_json(HEALTH_PATH, state)
    return proc.returncode


def seconds_until_next_run(daily_time: str, timezone_name: str) -> float:
    tz = ZoneInfo(timezone_name)
    now = datetime.now(tz)
    hour, minute = [int(part) for part in daily_time.split(":", 1)]
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return max(0.0, (target - now).total_seconds())


def run_scheduler_forever(config: AppConfig, script_path: str = "scripts/run_channel_30d_comparison.py") -> None:
    while True:
        wait_seconds = seconds_until_next_run(config.schedule.daily_time, config.schedule.timezone)
        logger.info("Next scheduled run in %.0f seconds", wait_seconds)
        time.sleep(wait_seconds)
        run_scheduled_job(config, script_path=script_path)
