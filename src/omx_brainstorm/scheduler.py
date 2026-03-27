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
from .youtube import YoutubeResolver
from .utils import write_json, read_json

logger = logging.getLogger(__name__)
HEALTH_PATH = Path(".omx/state/scheduler_health.json")
DEFAULT_COMPARISON_TARGET = "scripts.run_channel_30d_comparison"


def build_scheduler_command(config: AppConfig, target: str = DEFAULT_COMPARISON_TARGET) -> list[str]:
    if target.endswith(".py"):
        normalized = target[:-3].replace("/", ".")
        command = [sys.executable, "-m", normalized]
    elif "/" not in target and "." in target:
        command = [sys.executable, "-m", target]
    else:
        command = [sys.executable, target]
    if config.config_path:
        command.extend(["--config", config.config_path])
    return command


def run_scheduled_job(config: AppConfig, script_path: str = DEFAULT_COMPARISON_TARGET) -> int:
    logger.info("Running scheduled comparison job")
    state = read_json(HEALTH_PATH, {"error_count": 0})
    proc = subprocess.run(build_scheduler_command(config, script_path), text=True, capture_output=True, check=False)
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


def daily_run_due(config: AppConfig, state: dict, now: datetime | None = None) -> bool:
    if not config.schedule.enabled:
        return False
    tz = ZoneInfo(config.schedule.timezone)
    now = now or datetime.now(timezone.utc)
    localized_now = now.astimezone(tz)
    hour, minute = [int(part) for part in config.schedule.daily_time.split(":", 1)]
    scheduled = localized_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if localized_now < scheduled:
        return False
    return state.get("last_daily_run_local_date") != localized_now.date().isoformat()


def scan_channels_for_new_videos(
    config: AppConfig,
    state: dict,
    *,
    resolver: YoutubeResolver | None = None,
    now: datetime | None = None,
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    resolver = resolver or YoutubeResolver()
    now = now or datetime.now(timezone.utc)
    channel_state = state.setdefault("channels", {})
    retain_limit = max(1, config.schedule.poll_video_limit)
    has_prior_success = bool(read_json(HEALTH_PATH, {}).get("last_success_at"))
    bootstrap_mode = not any(entry.get("processed_video_ids") for entry in channel_state.values()) and has_prior_success
    current_ids_by_channel: dict[str, list[str]] = {}
    new_ids_by_channel: dict[str, list[str]] = {}

    for channel in config.channels:
        if not channel.enabled:
            continue
        entry = channel_state.setdefault(channel.slug, {})
        try:
            videos = resolver.resolve_channel_videos(channel.url, limit=retain_limit)
        except Exception as exc:
            logger.warning("Channel poll failed for %s: %s", channel.slug, exc)
            entry["last_poll_error"] = str(exc)
            entry["last_polled_at"] = now.isoformat()
            continue

        current_ids = [video.video_id for video in videos]
        current_ids_by_channel[channel.slug] = current_ids
        entry["last_polled_at"] = now.isoformat()
        entry["current_video_ids"] = current_ids
        entry["latest_published_at"] = next((video.published_at for video in videos if video.published_at), entry.get("latest_published_at", ""))
        processed_ids = list(entry.get("processed_video_ids", []))
        if bootstrap_mode and not processed_ids:
            entry["processed_video_ids"] = current_ids[:retain_limit]
            continue
        unseen = [video_id for video_id in current_ids if video_id not in set(processed_ids)]
        if unseen:
            new_ids_by_channel[channel.slug] = unseen
    state["last_poll_at"] = now.isoformat()
    return new_ids_by_channel, current_ids_by_channel


def mark_channels_processed(
    state: dict,
    current_ids_by_channel: dict[str, list[str]],
    *,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(timezone.utc)
    channel_state = state.setdefault("channels", {})
    for slug, current_ids in current_ids_by_channel.items():
        entry = channel_state.setdefault(slug, {})
        entry["processed_video_ids"] = list(current_ids)
        entry["last_processed_at"] = now.isoformat()


def run_scheduler_iteration(
    config: AppConfig,
    *,
    script_path: str = DEFAULT_COMPARISON_TARGET,
    resolver: YoutubeResolver | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    now = now or datetime.now(timezone.utc)
    state_path = Path(config.schedule.state_path)
    state = read_json(state_path, {"channels": {}})
    new_ids_by_channel, current_ids_by_channel = scan_channels_for_new_videos(config, state, resolver=resolver, now=now)
    should_run_daily = daily_run_due(config, state, now=now)

    if not new_ids_by_channel and not should_run_daily:
        write_json(state_path, state)
        return {
            "ran": False,
            "reason": "idle",
            "new_videos": {},
        }

    trigger = "daily_and_new_videos" if should_run_daily and new_ids_by_channel else "daily" if should_run_daily else "new_videos"
    state["last_trigger_at"] = now.isoformat()
    state["last_trigger"] = trigger
    state["last_new_videos"] = new_ids_by_channel

    exit_code = run_scheduled_job(config, script_path=script_path)
    if exit_code == 0:
        mark_channels_processed(state, current_ids_by_channel, now=now)
        if should_run_daily:
            localized_now = now.astimezone(ZoneInfo(config.schedule.timezone))
            state["last_daily_run_local_date"] = localized_now.date().isoformat()
    else:
        state["last_failed_trigger_at"] = now.isoformat()

    write_json(state_path, state)
    return {
        "ran": True,
        "reason": trigger,
        "new_videos": new_ids_by_channel,
        "exit_code": exit_code,
    }


def run_scheduler_forever(config: AppConfig, script_path: str = DEFAULT_COMPARISON_TARGET) -> None:
    poll_interval_seconds = max(60, int(config.schedule.poll_interval_minutes) * 60)
    while True:
        result = run_scheduler_iteration(config, script_path=script_path)
        if result["ran"]:
            logger.info("Scheduler iteration finished: %s", result)
        logger.info("Next scheduler poll in %s seconds", poll_interval_seconds)
        time.sleep(poll_interval_seconds)
