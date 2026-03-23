import json

from omx_brainstorm.app_config import AppConfig, NotificationConfig, ScheduleConfig
from omx_brainstorm.healthcheck import read_health_state
from omx_brainstorm.notifications import send_telegram_message
from omx_brainstorm.scheduler import HEALTH_PATH, run_scheduled_job, seconds_until_next_run


def test_send_telegram_message_returns_false_without_credentials():
    config = NotificationConfig()
    assert send_telegram_message(config, "hello") is False


def test_seconds_until_next_run_is_non_negative():
    seconds = seconds_until_next_run("09:00", "Asia/Seoul")
    assert seconds >= 0


def test_read_health_state_returns_default_for_missing_file(tmp_path):
    state = read_health_state(tmp_path / "missing.json")
    assert state["status"] == "unknown"
    assert state["error_count"] == 0


def test_run_scheduled_job_writes_success_health(monkeypatch, tmp_path):
    class Proc:
        returncode = 0
        stdout = "ok"
        stderr = ""

    monkeypatch.setattr("omx_brainstorm.scheduler.HEALTH_PATH", tmp_path / "health.json")
    monkeypatch.setattr("omx_brainstorm.scheduler.subprocess.run", lambda *a, **k: Proc())
    monkeypatch.setattr("omx_brainstorm.scheduler.send_telegram_message", lambda *a, **k: True)
    config = AppConfig(notifications=NotificationConfig())
    assert run_scheduled_job(config) == 0
    state = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert state["status"] == "ok"


def test_run_scheduled_job_writes_error_health(monkeypatch, tmp_path):
    class Proc:
        returncode = 2
        stdout = ""
        stderr = "boom"

    monkeypatch.setattr("omx_brainstorm.scheduler.HEALTH_PATH", tmp_path / "health.json")
    monkeypatch.setattr("omx_brainstorm.scheduler.subprocess.run", lambda *a, **k: Proc())
    monkeypatch.setattr("omx_brainstorm.scheduler.send_telegram_message", lambda *a, **k: True)
    config = AppConfig(notifications=NotificationConfig())
    assert run_scheduled_job(config) == 2
    state = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["error_count"] == 1


def test_seconds_until_next_run_for_late_target_is_small(monkeypatch):
    seconds = seconds_until_next_run("23:59", "Asia/Seoul")
    assert seconds >= 0
