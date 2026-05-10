from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .utils import read_json

DEFAULT_HEALTH_PATH = Path(".omx/state/scheduler_health.json")
DEFAULT_STALE_THRESHOLD_HOURS = 6.0


def read_health_state(path: str | Path = DEFAULT_HEALTH_PATH) -> dict[str, Any]:
    """Load the scheduler health JSON, returning a default skeleton if missing."""
    return read_json(Path(path), {"status": "unknown", "error_count": 0})


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def compute_health_summary(
    state: dict[str, Any],
    *,
    stale_threshold_hours: float = DEFAULT_STALE_THRESHOLD_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Augment a raw health-state dict with staleness diagnostics.

    Returns a new dict containing the original state plus ``staleness_hours``,
    ``is_stale``, ``stale_threshold_hours``, and ``healthcheck_at`` keys. A
    state with no recorded ``last_success_at`` is treated as stale.
    """
    now = now or datetime.now(timezone.utc)
    last_success = _parse_iso(state.get("last_success_at"))
    if last_success is None:
        staleness_hours: float | None = None
        is_stale = True
    else:
        delta = now - last_success
        staleness_hours = round(delta.total_seconds() / 3600.0, 2)
        is_stale = delta > timedelta(hours=stale_threshold_hours)

    summary = dict(state)
    summary["healthcheck_at"] = now.isoformat()
    summary["stale_threshold_hours"] = float(stale_threshold_hours)
    summary["staleness_hours"] = staleness_hours
    summary["is_stale"] = bool(is_stale)
    return summary
