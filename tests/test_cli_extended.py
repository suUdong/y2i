"""Extended CLI tests for register-channel, analyze-channel-30d, backtest-ranked, run-comparison handlers."""
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from omx_brainstorm.cli import main


def test_cli_register_channel(monkeypatch, tmp_path, capsys):
    registry_path = tmp_path / "channels.json"

    mock_resolver = MagicMock()
    mock_video = MagicMock()
    mock_video.channel_title = "Test Channel"
    mock_video.channel_id = "UC123"
    mock_resolver.resolve_channel_videos.return_value = [mock_video]

    mock_registry = MagicMock()
    mock_registry.register.return_value = {"url": "https://youtube.com/@test", "channel_id": "UC123"}

    monkeypatch.setattr("omx_brainstorm.cli.YoutubeResolver", lambda: mock_resolver)
    monkeypatch.setattr("omx_brainstorm.cli.ChannelRegistry", lambda p: mock_registry)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "register-channel", "https://youtube.com/@test",
        "--registry", str(registry_path),
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["channel_id"] == "UC123"


def test_cli_register_channel_no_videos(monkeypatch, tmp_path, capsys):
    mock_resolver = MagicMock()
    mock_resolver.resolve_channel_videos.return_value = []

    mock_registry = MagicMock()
    mock_registry.register.return_value = {"url": "https://youtube.com/@test", "channel_id": None}

    monkeypatch.setattr("omx_brainstorm.cli.YoutubeResolver", lambda: mock_resolver)
    monkeypatch.setattr("omx_brainstorm.cli.ChannelRegistry", lambda p: mock_registry)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "register-channel", "https://youtube.com/@test",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["channel_id"] is None


def test_cli_backtest_ranked(monkeypatch, tmp_path, capsys):
    artifact = tmp_path / "ranking.json"
    artifact.write_text(json.dumps({
        "cross_video_ranking": [
            {"ticker": "NVDA", "company_name": "NVIDIA", "aggregate_score": 80.0, "first_signal_at": "2026-03-01"},
        ]
    }), encoding="utf-8")

    @dataclass
    class _FakeReport:
        def to_dict(self):
            return {"portfolio_return_pct": 5.0, "positions": []}

    mock_engine = MagicMock()
    mock_engine.run_buy_and_hold.return_value = _FakeReport()
    monkeypatch.setattr("omx_brainstorm.cli.BacktestEngine", lambda: mock_engine)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "backtest-ranked", str(artifact),
        "--start-date", "2026-03-01", "--end-date", "2026-03-20",
        "--top-n", "3",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["portfolio_return_pct"] == 5.0


def test_cli_analyze_channel_30d_handler(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    config_path.write_text("""
[app]
provider = "mock"
output_dir = "{output}"

[logging]
json = false
log_dir = "logs"
retention_days = 7

[[channels]]
slug = "sampro"
display_name = "삼프로TV"
url = "https://youtube.com/@3protv/videos"
enabled = true
""".format(output=str(output_dir)), encoding="utf-8")

    mock_resolver = MagicMock()
    mock_resolver.resolve_channel_videos_since.return_value = []

    monkeypatch.setattr("omx_brainstorm.cli.YoutubeResolver", lambda: mock_resolver)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(output_dir),
        "analyze-channel-30d", "sampro", "--config", str(config_path), "--days", "7",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["video_count"] == 0
    assert output["dashboard_path"] is None


def test_cli_analyze_channel_30d_unknown_slug(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[[channels]]
slug = "other"
display_name = "Other"
url = "https://youtube.com/@other"
""", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-channel-30d", "nonexistent", "--config", str(config_path),
    ])

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1


def test_cli_run_comparison(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[logging]
json = false
log_dir = "logs"
retention_days = 7

[[channels]]
slug = "test"
display_name = "Test"
url = "https://youtube.com/@test"
""", encoding="utf-8")

    mock_job = MagicMock(return_value={"status": "ok", "channels": {}})
    monkeypatch.setattr("omx_brainstorm.cli.load_app_config", lambda p: __import__("omx_brainstorm.app_config", fromlist=["load_app_config"]).load_app_config(config_path))

    # Mock the import of run_comparison_job
    import types
    fake_module = types.ModuleType("scripts.run_channel_30d_comparison")
    fake_module.run_comparison_job = mock_job
    monkeypatch.setitem(sys.modules, "scripts.run_channel_30d_comparison", fake_module)

    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "run-comparison", "--config", str(config_path),
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "ok"


def test_cli_run_scheduler_once(monkeypatch, tmp_path):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[logging]
json = false
log_dir = "logs"
retention_days = 7
""", encoding="utf-8")

    monkeypatch.setattr("omx_brainstorm.cli.run_scheduled_job", lambda config: 0)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "run-scheduler", "--config", str(config_path), "--once",
    ])

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0
