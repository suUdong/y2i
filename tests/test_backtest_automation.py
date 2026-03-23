import json
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

from omx_brainstorm.backtest_automation import run_backtest_for_artifact


def test_run_backtest_for_artifact_handles_empty_ranking(tmp_path):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({"cross_video_ranking": []}), encoding="utf-8")

    result = run_backtest_for_artifact(artifact)

    assert result["status"] == "no_ranking"
    assert result["backtest_report"] is None


def test_run_backtest_for_artifact_handles_missing_signal_dates(tmp_path):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({
        "cross_video_ranking": [
            {"ticker": "NVDA", "company_name": "NVIDIA", "aggregate_score": 80.0},
            {"ticker": "AAPL", "company_name": "Apple", "aggregate_score": 70.0},
        ]
    }), encoding="utf-8")

    result = run_backtest_for_artifact(artifact)

    assert result["status"] == "missing_signal_dates"
    assert result["backtest_report"] is None


def test_run_backtest_for_artifact_success(tmp_path, monkeypatch):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({
        "cross_video_ranking": [
            {"ticker": "NVDA", "company_name": "NVIDIA", "aggregate_score": 80.0, "first_signal_at": "2026-03-01"},
            {"ticker": "AAPL", "company_name": "Apple", "aggregate_score": 70.0, "first_signal_at": "2026-03-05"},
        ]
    }), encoding="utf-8")

    @dataclass
    class _FakeReport:
        def to_dict(self):
            return {"portfolio_return_pct": 5.2, "positions": []}

    class _FakeEngine:
        def run_buy_and_hold(self, **kwargs):
            assert kwargs["start_date"] == "2026-03-01"
            assert kwargs["top_n"] is None
            return _FakeReport()

    monkeypatch.setattr("omx_brainstorm.backtest_automation.BacktestEngine", _FakeEngine)

    result = run_backtest_for_artifact(artifact)

    assert result["status"] == "ok"
    assert result["backtest_report"]["portfolio_return_pct"] == 5.2


def test_run_backtest_for_artifact_with_top_n(tmp_path, monkeypatch):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({
        "cross_video_ranking": [
            {"ticker": "NVDA", "aggregate_score": 80.0, "first_signal_at": "2026-03-01"},
        ]
    }), encoding="utf-8")

    @dataclass
    class _FakeReport:
        def to_dict(self):
            return {"portfolio_return_pct": 3.0, "positions": []}

    class _FakeEngine:
        def run_buy_and_hold(self, **kwargs):
            assert kwargs["top_n"] == 3
            assert kwargs["initial_capital"] == 50000.0
            return _FakeReport()

    monkeypatch.setattr("omx_brainstorm.backtest_automation.BacktestEngine", _FakeEngine)

    result = run_backtest_for_artifact(artifact, top_n=3, initial_capital=50000.0)

    assert result["status"] == "ok"


def test_run_backtest_for_artifact_no_cross_video_ranking_key(tmp_path):
    artifact = tmp_path / "artifact.json"
    artifact.write_text(json.dumps({"some_other_key": []}), encoding="utf-8")

    result = run_backtest_for_artifact(artifact)

    assert result["status"] == "no_ranking"
