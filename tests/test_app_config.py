import os
import pytest

from omx_brainstorm.app_config import load_app_config


def test_load_app_config_uses_defaults_when_file_missing(tmp_path):
    config = load_app_config(tmp_path / "missing.toml")
    assert config.channels
    assert any(channel.slug == "sampro" for channel in config.channels)
    assert config.strategy.window_days == 30


def test_load_app_config_uses_toml_and_env_override(tmp_path, monkeypatch):
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        """
[app]
provider = "mock"

[[channels]]
slug = "demo"
display_name = "Demo"
url = "https://youtube.com/@demo"

[strategy]
window_days = 15
        """.strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("OMX_WINDOW_DAYS", "45")

    config = load_app_config(config_path)

    assert config.provider == "mock"
    assert config.channels[0].slug == "demo"
    assert config.strategy.window_days == 45


def test_load_app_config_raises_on_invalid_toml(tmp_path):
    path = tmp_path / "broken.toml"
    path.write_text("[app\nprovider='x'", encoding="utf-8")
    with pytest.raises(ValueError):
        load_app_config(path)


def test_load_app_config_raises_on_missing_channel_slug(tmp_path):
    path = tmp_path / "bad.toml"
    path.write_text(
        """
[[channels]]
display_name = "Broken"
url = "https://youtube.com/@broken"
        """.strip(),
        encoding="utf-8",
    )
    with pytest.raises(ValueError):
        load_app_config(path)


def test_load_app_config_reads_logging_section(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text(
        """
[logging]
json = false
log_dir = "logs"
retention_days = 3
        """.strip(),
        encoding="utf-8",
    )
    config = load_app_config(path)
    assert config.logging.json is False
    assert config.logging.log_dir == "logs"
    assert config.logging.retention_days == 3


def test_load_app_config_reads_scheduler_extensions(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text(
        """
[schedule]
enabled = true
daily_time = "08:30"
timezone = "Asia/Seoul"
poll_interval_minutes = 15
poll_video_limit = 6
state_path = ".omx/state/custom-scheduler.json"
        """.strip(),
        encoding="utf-8",
    )
    config = load_app_config(path)
    assert config.config_path == str(path)
    assert config.schedule.enabled is True
    assert config.schedule.poll_interval_minutes == 15
    assert config.schedule.poll_video_limit == 6
    assert config.schedule.state_path == ".omx/state/custom-scheduler.json"


def test_load_app_config_reads_parallel_and_retry_settings(tmp_path):
    path = tmp_path / "cfg.toml"
    path.write_text(
        """
[strategy]
video_workers = 6
fundamentals_workers = 3

[schedule]
job_max_attempts = 5
retry_backoff_seconds = 45
        """.strip(),
        encoding="utf-8",
    )
    config = load_app_config(path)
    assert config.strategy.video_workers == 6
    assert config.strategy.fundamentals_workers == 3
    assert config.schedule.job_max_attempts == 5
    assert config.schedule.retry_backoff_seconds == 45


def test_load_app_config_env_overrides_logging(tmp_path, monkeypatch):
    path = tmp_path / "cfg.toml"
    path.write_text("[logging]\njson = false", encoding="utf-8")
    monkeypatch.setenv("OMX_JSON_LOGS", "true")
    monkeypatch.setenv("OMX_LOG_RETENTION_DAYS", "9")
    config = load_app_config(path)
    assert config.logging.json is True
    assert config.logging.retention_days == 9
