"""Integration tests for signal tracker + alerts wired into the comparison pipeline."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from omx_brainstorm.app_config import NotificationConfig, StrategyConfig
from omx_brainstorm.channel_quality import compute_channel_quality, rank_channels
from omx_brainstorm.signal_alerts import filter_high_quality_signals, send_signal_alerts
from omx_brainstorm.signal_tracker import SignalTrackerDB, record_signals_from_output, update_price_snapshots
from omx_brainstorm.backtest import HistoricalPricePoint


class FakeHistoryProvider:
    def __init__(self, prices: dict[str, list[HistoricalPricePoint]] | None = None):
        self.prices = prices or {}

    def get_price_history(self, ticker: str, start_date: str, end_date: str) -> list[HistoricalPricePoint]:
        return self.prices.get(ticker, [])


@pytest.fixture
def tracker_db(tmp_path: Path) -> SignalTrackerDB:
    return SignalTrackerDB(tmp_path / "tracker.json")


@pytest.fixture
def channel_output(tmp_path: Path) -> Path:
    data = {
        "channel_slug": "itgod",
        "cross_video_ranking": [
            {
                "ticker": "005930.KS",
                "company_name": "삼성전자",
                "aggregate_score": 78.5,
                "aggregate_verdict": "BUY",
                "first_signal_at": "2026-03-15",
                "latest_price": 58000.0,
                "currency": "KRW",
                "appearances": 3,
                "total_mentions": 5,
            },
            {
                "ticker": "000660.KS",
                "company_name": "SK하이닉스",
                "aggregate_score": 55.0,
                "aggregate_verdict": "WATCH",
                "first_signal_at": "2026-03-18",
                "latest_price": 120000.0,
                "currency": "KRW",
                "appearances": 1,
                "total_mentions": 2,
            },
        ],
    }
    path = tmp_path / "itgod_30d_test.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_end_to_end_track_and_alert(tracker_db: SignalTrackerDB, channel_output: Path):
    """Simulate the scheduler post-run flow: track signals, compute quality, send alerts."""
    provider = FakeHistoryProvider(
        {
            "005930.KS": [HistoricalPricePoint(date="2026-03-15", close=58000.0)],
            "000660.KS": [HistoricalPricePoint(date="2026-03-18", close=120000.0)],
        }
    )
    # Step 1: Record signals from output
    added = record_signals_from_output(tracker_db, channel_output, history_provider=provider)
    assert added == 2
    assert len(tracker_db.records) == 2

    # Step 2: Compute channel quality
    channel_comparison = {
        "itgod": {
            "display_name": "IT의 신",
            "actionable_ratio": 0.733,
            "ranking_spearman": 0.5,
            "quality_scorecard": {
                "overall": 67.3,
                "transcript_coverage": 100.0,
                "actionable_density": 88.0,
                "ranking_predictive_power": 47.6,
                "horizon_adequacy": 33.6,
            },
        },
    }
    accuracy = {"itgod": tracker_db.accuracy_report("itgod").to_dict()}
    quality_reports = compute_channel_quality(channel_comparison, accuracy)
    ranked = rank_channels(quality_reports)
    assert len(ranked) == 1
    assert ranked[0].slug == "itgod"
    assert ranked[0].overall_quality_score > 0

    # Step 3: Filter signals for alerts
    quality_scores = {r.slug: r.overall_quality_score for r in ranked}
    ranking_data = json.loads(channel_output.read_text())["cross_video_ranking"]
    signals = filter_high_quality_signals(
        ranking_data,
        channel_quality_scores=quality_scores,
        channel_slug="itgod",
        min_score=68.0,
        min_channel_quality=50.0,
    )
    # Only 005930.KS (score 78.5) should pass the 68.0 threshold
    assert len(signals) == 1
    assert signals[0]["ticker"] == "005930.KS"


def test_strategy_config_alert_fields():
    """Verify new strategy config fields have correct defaults."""
    config = StrategyConfig()
    assert config.signal_alert_min_score == 68.0
    assert config.signal_alert_min_channel_quality == 50.0


def test_tracker_db_survives_reload(tracker_db: SignalTrackerDB, channel_output: Path):
    """Verify signal records persist across DB reloads."""
    provider = FakeHistoryProvider(
        {
            "005930.KS": [HistoricalPricePoint(date="2026-03-15", close=58000.0)],
            "000660.KS": [HistoricalPricePoint(date="2026-03-18", close=120000.0)],
        }
    )
    record_signals_from_output(tracker_db, channel_output, history_provider=provider)
    db2 = SignalTrackerDB(tracker_db.db_path)
    assert len(db2.records) == 2
    assert db2.records[0].ticker == "005930.KS"


@patch("omx_brainstorm.signal_alerts._send_telegram_html")
def test_alert_send_with_quality_gate(mock_send, tracker_db: SignalTrackerDB, channel_output: Path):
    """Verify alerts are sent only for qualifying channels."""
    mock_send.return_value = True
    config = NotificationConfig(telegram_bot_token="test", telegram_chat_id="123")
    ranking_data = json.loads(channel_output.read_text())["cross_video_ranking"]

    # High-quality channel passes
    result = send_signal_alerts(
        config, ranking_data, channel_name="IT의 신", channel_slug="itgod",
        channel_quality_scores={"itgod": 70.0}, min_score=68.0,
    )
    assert result is True
    assert mock_send.called

    mock_send.reset_mock()

    # Low-quality channel blocked
    result = send_signal_alerts(
        config, ranking_data, channel_name="Low", channel_slug="low",
        channel_quality_scores={"low": 30.0}, min_score=68.0,
    )
    assert result is False
    assert not mock_send.called
