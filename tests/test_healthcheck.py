import json
from datetime import datetime, timedelta, timezone

from omx_brainstorm.healthcheck import compute_health_summary, read_health_state


_FROZEN_NOW = datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc)


def test_read_health_state_reads_existing_file(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(json.dumps({"status": "ok", "error_count": 2}), encoding="utf-8")
    state = read_health_state(path)
    assert state["status"] == "ok"
    assert state["error_count"] == 2


def test_read_health_state_default_has_unknown_status(tmp_path):
    state = read_health_state(tmp_path / "missing.json")
    assert state["status"] == "unknown"


def test_read_health_state_default_has_zero_errors(tmp_path):
    state = read_health_state(tmp_path / "missing.json")
    assert state["error_count"] == 0


def test_read_health_state_works_with_string_path(tmp_path):
    path = tmp_path / "health.json"
    path.write_text(json.dumps({"status": "ok"}), encoding="utf-8")
    state = read_health_state(str(path))
    assert state["status"] == "ok"


def test_compute_health_summary_marks_fresh_state_healthy():
    state = {
        "last_success_at": (_FROZEN_NOW - timedelta(hours=1)).isoformat(),
        "status": "ok",
    }
    summary = compute_health_summary(state, stale_threshold_hours=6.0, now=_FROZEN_NOW)
    assert summary["is_stale"] is False
    assert summary["staleness_hours"] == 1.0
    assert summary["stale_threshold_hours"] == 6.0
    assert summary["status"] == "ok"


def test_compute_health_summary_marks_stale_state_when_past_threshold():
    state = {
        "last_success_at": (_FROZEN_NOW - timedelta(hours=24)).isoformat(),
        "status": "ok",
    }
    summary = compute_health_summary(state, stale_threshold_hours=6.0, now=_FROZEN_NOW)
    assert summary["is_stale"] is True
    assert summary["staleness_hours"] == 24.0


def test_compute_health_summary_missing_last_success_is_stale():
    summary = compute_health_summary(
        {"status": "unknown"}, stale_threshold_hours=6.0, now=_FROZEN_NOW
    )
    assert summary["is_stale"] is True
    assert summary["staleness_hours"] is None


def test_compute_health_summary_preserves_unrelated_fields():
    state = {
        "last_success_at": _FROZEN_NOW.isoformat(),
        "status": "ok",
        "error_count": 42,
        "last_error": "yfinance flake",
    }
    summary = compute_health_summary(state, now=_FROZEN_NOW)
    assert summary["error_count"] == 42
    assert summary["last_error"] == "yfinance flake"


def test_compute_health_summary_accepts_z_suffix_iso():
    state = {"last_success_at": "2026-05-10T22:00:00Z", "status": "ok"}
    summary = compute_health_summary(state, stale_threshold_hours=6.0, now=_FROZEN_NOW)
    assert summary["is_stale"] is False
    assert summary["staleness_hours"] == 2.0
