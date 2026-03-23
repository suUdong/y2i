"""Tests for CLI subcommands: analyze-video, analyze-channel, list-channels, healthcheck, analyze-all."""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

from omx_brainstorm.cli import main
from omx_brainstorm.models import (
    VideoAnalysisReport, VideoInput, VideoSignalAssessment,
)


def _make_mock_report(tmp_path):
    video = VideoInput(video_id="v1", title="테스트", url="https://youtube.com/watch?v=v1")
    signal = VideoSignalAssessment(
        signal_score=75.0, video_signal_class="ACTIONABLE",
        should_analyze_stocks=True, reason="test", video_type="STOCK_PICK",
    )
    report = VideoAnalysisReport(
        run_id="r1", created_at="2026-03-24", provider="mock", mode="ralph",
        video=video, signal_assessment=signal, transcript_text="t", transcript_language="ko",
        ticker_mentions=[], stock_analyses=[], macro_insights=[], expert_insights=[],
    )
    paths = (tmp_path / "a.json", tmp_path / "a.md", tmp_path / "a.txt")
    for p in paths:
        p.write_text("{}", encoding="utf-8")
    return report, paths


def test_cli_analyze_video(monkeypatch, tmp_path, capsys):
    report, paths = _make_mock_report(tmp_path)

    mock_pipeline = MagicMock()
    mock_pipeline.analyze_video.return_value = (report, paths)

    monkeypatch.setattr("omx_brainstorm.cli.OMXPipeline", lambda **kw: mock_pipeline)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-video", "https://youtube.com/watch?v=v1",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["video"] == "테스트"
    assert output["video_type"] == "STOCK_PICK"


def test_cli_analyze_channel(monkeypatch, tmp_path, capsys):
    report, paths = _make_mock_report(tmp_path)

    mock_pipeline = MagicMock()
    mock_pipeline.analyze_channel.return_value = [(report, paths)]

    monkeypatch.setattr("omx_brainstorm.cli.OMXPipeline", lambda **kw: mock_pipeline)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-channel", "https://youtube.com/@test", "--limit", "1",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert len(output) == 1
    assert output[0]["video"] == "테스트"


def test_cli_list_channels(monkeypatch, tmp_path, capsys):
    registry_path = tmp_path / "channels.json"
    registry_path.write_text('[{"url": "https://test", "channel_id": "C1"}]', encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["omx-brainstorm", "list-channels", "--registry", str(registry_path)])

    main()
    output = json.loads(capsys.readouterr().out)
    assert len(output) == 1
    assert output[0]["url"] == "https://test"


def test_cli_run_healthcheck(monkeypatch, tmp_path, capsys):
    health_path = tmp_path / "health.json"
    health_path.write_text('{"status": "ok", "last_run": "2026-03-24"}', encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["omx-brainstorm", "run-healthcheck", "--path", str(health_path)])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "ok"


def test_cli_analyze_all_no_channels(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[logging]
json = false
log_dir = "logs"
retention_days = 7

[[channels]]
slug = "disabled_ch"
display_name = "Disabled"
url = "https://youtube.com/@disabled"
enabled = false
""", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-all", "--config", str(config_path),
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["error"] == "no enabled channels"


def test_cli_analyze_all_with_channel(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[logging]
json = false
log_dir = "logs"
retention_days = 7

[[channels]]
slug = "test_ch"
display_name = "Test Channel"
url = "https://youtube.com/@test"
enabled = true
""", encoding="utf-8")

    report, paths = _make_mock_report(tmp_path)
    mock_pipeline = MagicMock()
    mock_pipeline.analyze_channel.return_value = [(report, paths)]

    monkeypatch.setattr("omx_brainstorm.cli.OMXPipeline", lambda **kw: mock_pipeline)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-all", "--config", str(config_path), "--limit", "1",
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert "test_ch" in output
    assert output["test_ch"]["videos_analyzed"] == 1


def test_cli_analyze_all_channel_error(monkeypatch, tmp_path, capsys):
    config_path = tmp_path / "config.toml"
    config_path.write_text("""
[app]
provider = "mock"

[logging]
json = false
log_dir = "logs"
retention_days = 7

[[channels]]
slug = "broken_ch"
display_name = "Broken"
url = "https://youtube.com/@broken"
enabled = true
""", encoding="utf-8")

    mock_pipeline = MagicMock()
    mock_pipeline.analyze_channel.side_effect = RuntimeError("network error")

    monkeypatch.setattr("omx_brainstorm.cli.OMXPipeline", lambda **kw: mock_pipeline)
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-all", "--config", str(config_path),
    ])

    main()
    output = json.loads(capsys.readouterr().out)
    assert "error" in output["broken_ch"]


def test_cli_backtest_artifact(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr("omx_brainstorm.cli.run_backtest_for_artifact", lambda *a, **kw: {"result": "ok"})
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(sys, "argv", ["omx-brainstorm", "backtest-artifact", str(artifact)])

    main()
    output = json.loads(capsys.readouterr().out)
    assert output["result"] == "ok"


def test_cli_error_exits_with_code_1(monkeypatch, tmp_path):
    monkeypatch.setattr("omx_brainstorm.cli.OMXPipeline", MagicMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(sys, "argv", [
        "omx-brainstorm", "--output-dir", str(tmp_path),
        "analyze-video", "https://youtube.com/watch?v=bad",
    ])

    import pytest
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
