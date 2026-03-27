import json
from datetime import datetime, timezone

import sys

from omx_brainstorm.app_config import AppConfig, ChannelConfig, NotificationConfig, ScheduleConfig
from omx_brainstorm.healthcheck import read_health_state
from omx_brainstorm.models import VideoInput
from omx_brainstorm.notifications import send_telegram_message
from omx_brainstorm.scheduler import (
    HEALTH_PATH,
    build_scheduler_command,
    daily_run_due,
    processed_ids_from_payload,
    run_scheduled_job,
    run_scheduler_iteration,
    scan_channels_for_new_videos,
    seconds_until_next_run,
)


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
    monkeypatch.setattr("omx_brainstorm.scheduler.notify_all", lambda *a, **k: {"telegram": True, "discord": True})
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
    monkeypatch.setattr("omx_brainstorm.scheduler.notify_all", lambda *a, **k: {"telegram": True, "discord": True})
    config = AppConfig(notifications=NotificationConfig())
    assert run_scheduled_job(config) == 2
    state = json.loads((tmp_path / "health.json").read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["error_count"] == 1


def test_build_scheduler_command_uses_module_and_config_path():
    config = AppConfig(config_path="/tmp/config.toml")
    command = build_scheduler_command(config)
    assert command[:3] == [sys.executable, "-m", "scripts.run_channel_30d_comparison"]
    assert command[-2:] == ["--config", "/tmp/config.toml"]


def test_daily_run_due_after_schedule_time():
    config = AppConfig(schedule=ScheduleConfig(enabled=True, daily_time="09:00", timezone="Asia/Seoul"))
    now = datetime(2026, 3, 27, 1, 0, tzinfo=timezone.utc)
    assert daily_run_due(config, {}, now=now) is True


def test_scan_channels_for_new_videos_bootstraps_after_prior_success(monkeypatch):
    config = AppConfig(
        channels=[ChannelConfig(
            slug="sampro",
            display_name="Sampro",
            url="https://youtube.com/@sampro",
        )],
        schedule=ScheduleConfig(poll_video_limit=3),
    )

    class Resolver:
        def resolve_channel_videos(self, channel_url, limit):
            return [
                VideoInput(video_id="vid-new", title="New", url="u", published_at="20260327"),
                VideoInput(video_id="vid-old", title="Old", url="u", published_at="20260326"),
            ]

    monkeypatch.setattr("omx_brainstorm.scheduler.read_json", lambda path, default: {"last_success_at": "2026-03-27T00:41:08+00:00"} if path == HEALTH_PATH else default)
    state = {"channels": {}}
    new_ids, current_ids = scan_channels_for_new_videos(config, state, resolver=Resolver(), now=datetime(2026, 3, 27, 1, 5, tzinfo=timezone.utc))
    assert new_ids == {}
    assert current_ids["sampro"] == ["vid-new", "vid-old"]
    assert state["channels"]["sampro"]["processed_video_ids"] == ["vid-new", "vid-old"]


def test_scan_channels_for_new_videos_detects_unprocessed_upload(monkeypatch):
    config = AppConfig(
        channels=[ChannelConfig(
            slug="sampro",
            display_name="Sampro",
            url="https://youtube.com/@sampro",
        )],
        schedule=ScheduleConfig(poll_video_limit=3),
    )

    class Resolver:
        def resolve_channel_videos(self, channel_url, limit):
            return [
                VideoInput(video_id="vid-new", title="New", url="u", published_at="20260327"),
                VideoInput(video_id="vid-old", title="Old", url="u", published_at="20260326"),
            ]

    monkeypatch.setattr("omx_brainstorm.scheduler.read_json", lambda path, default: {} if path == HEALTH_PATH else default)
    state = {"channels": {"sampro": {"processed_video_ids": ["vid-old"]}}}
    new_ids, _current_ids = scan_channels_for_new_videos(config, state, resolver=Resolver(), now=datetime(2026, 3, 27, 1, 5, tzinfo=timezone.utc))
    assert new_ids == {"sampro": ["vid-new"]}


def test_run_scheduler_iteration_triggers_and_updates_state(monkeypatch, tmp_path):
    state_path = tmp_path / "scheduler_state.json"
    health_path = tmp_path / "health.json"
    monkeypatch.setattr("omx_brainstorm.scheduler.HEALTH_PATH", health_path)

    config = AppConfig(
        config_path=str(tmp_path / "config.toml"),
        channels=[ChannelConfig(
            slug="sampro",
            display_name="Sampro",
            url="https://youtube.com/@sampro",
        )],
        schedule=ScheduleConfig(enabled=True, daily_time="09:00", timezone="Asia/Seoul", poll_video_limit=3, state_path=str(state_path)),
        notifications=NotificationConfig(),
    )

    class Resolver:
        def resolve_channel_videos(self, channel_url, limit):
            return [
                VideoInput(video_id="vid-new", title="New", url="u", published_at="20260327"),
                VideoInput(video_id="vid-blocked", title="Blocked", url="u", published_at="20260327"),
            ]

    artifact_path = tmp_path / "sampro_30d_test.json"
    artifact_path.write_text(json.dumps({"videos": [{"video_id": "vid-new"}]}), encoding="utf-8")

    class Proc:
        returncode = 0
        stdout = json.dumps({"channels": {"sampro": {"json_path": str(artifact_path), "txt_path": str(tmp_path / "sampro_30d_test.txt")}}})
        stderr = ""

    monkeypatch.setattr("omx_brainstorm.scheduler.subprocess.run", lambda *a, **k: Proc())
    monkeypatch.setattr("omx_brainstorm.scheduler.notify_all", lambda *a, **k: {"telegram": False, "discord": False})
    result = run_scheduler_iteration(config, resolver=Resolver(), now=datetime(2026, 3, 27, 1, 5, tzinfo=timezone.utc))
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert result["ran"] is True
    assert result["reason"] == "daily_and_new_videos"
    assert state["channels"]["sampro"]["processed_video_ids"] == ["vid-new"]
    assert state["last_daily_run_local_date"] == "2026-03-27"


def test_processed_ids_from_payload_reads_channel_artifacts(tmp_path):
    artifact_path = tmp_path / "sampro_30d_test.json"
    artifact_path.write_text(json.dumps({"videos": [{"video_id": "vid-1"}, {"video_id": "vid-2"}]}), encoding="utf-8")
    payload = {"channels": {"sampro": {"json_path": str(artifact_path)}}}
    assert processed_ids_from_payload(payload) == {"sampro": ["vid-1", "vid-2"]}


def test_seconds_until_next_run_for_late_target_is_small(monkeypatch):
    seconds = seconds_until_next_run("23:59", "Asia/Seoul")
    assert seconds >= 0
