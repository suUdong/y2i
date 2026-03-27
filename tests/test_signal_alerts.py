from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from omx_brainstorm.app_config import NotificationConfig
from omx_brainstorm.signal_alerts import (
    filter_high_quality_signals,
    format_telegram_alert,
    send_signal_alerts,
    DEFAULT_MIN_SCORE,
    DEFAULT_MIN_CHANNEL_QUALITY,
)


def _make_stock(ticker: str = "005930.KS", name: str = "삼성전자", score: float = 75.0,
                verdict: str = "BUY", price: float = 58000.0, currency: str = "KRW",
                appearances: int = 3, mentions: int = 5) -> dict:
    return {
        "ticker": ticker,
        "company_name": name,
        "aggregate_score": score,
        "aggregate_verdict": verdict,
        "latest_price": price,
        "currency": currency,
        "appearances": appearances,
        "total_mentions": mentions,
    }


class TestFilterHighQualitySignals:
    def test_filters_below_threshold(self):
        stocks = [_make_stock(score=80.0), _make_stock(ticker="X", score=50.0)]
        result = filter_high_quality_signals(stocks)
        assert len(result) == 1
        assert result[0]["ticker"] == "005930.KS"

    def test_all_pass(self):
        stocks = [_make_stock(score=80.0), _make_stock(ticker="X", score=70.0)]
        result = filter_high_quality_signals(stocks)
        assert len(result) == 2

    def test_channel_quality_gate(self):
        stocks = [_make_stock(score=80.0)]
        quality = {"bad_channel": 30.0}
        result = filter_high_quality_signals(
            stocks, channel_quality_scores=quality, channel_slug="bad_channel",
        )
        assert len(result) == 0

    def test_channel_quality_passes(self):
        stocks = [_make_stock(score=80.0)]
        quality = {"good_channel": 70.0}
        result = filter_high_quality_signals(
            stocks, channel_quality_scores=quality, channel_slug="good_channel",
        )
        assert len(result) == 1

    def test_no_channel_quality_data(self):
        stocks = [_make_stock(score=80.0)]
        result = filter_high_quality_signals(stocks, channel_slug="itgod")
        assert len(result) == 1

    def test_custom_thresholds(self):
        stocks = [_make_stock(score=60.0)]
        result = filter_high_quality_signals(stocks, min_score=55.0)
        assert len(result) == 1
        result = filter_high_quality_signals(stocks, min_score=65.0)
        assert len(result) == 0

    def test_empty_stocks(self):
        assert filter_high_quality_signals([]) == []


class TestFormatTelegramAlert:
    def test_basic_format(self):
        signals = [_make_stock()]
        msg = format_telegram_alert(signals, channel_name="IT의 신")
        assert "<b>🔔 Y2I 고품질 시그널 알림</b>" in msg
        assert "IT의 신" in msg
        assert "삼성전자" in msg
        assert "005930.KS" in msg
        assert "BUY" in msg

    def test_empty_signals(self):
        assert format_telegram_alert([]) == ""

    def test_html_tags(self):
        signals = [_make_stock()]
        msg = format_telegram_alert(signals)
        assert "<b>" in msg
        assert "</b>" in msg

    def test_multiple_signals(self):
        signals = [_make_stock(ticker="A", name="StockA"), _make_stock(ticker="B", name="StockB")]
        msg = format_telegram_alert(signals)
        assert "1." in msg
        assert "2." in msg

    def test_no_channel_name(self):
        signals = [_make_stock()]
        msg = format_telegram_alert(signals)
        assert "📺" not in msg


class TestSendSignalAlerts:
    def test_no_qualifying_signals(self):
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        stocks = [_make_stock(score=50.0)]  # Below threshold
        result = send_signal_alerts(config, stocks)
        assert result is False

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_sends_qualifying_signals(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        stocks = [_make_stock(score=80.0)]
        result = send_signal_alerts(config, stocks, channel_name="Test")
        assert result is True
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][1]
        assert "삼성전자" in msg

    def test_missing_credentials(self):
        config = NotificationConfig()
        stocks = [_make_stock(score=80.0)]
        result = send_signal_alerts(config, stocks)
        assert result is False

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_channel_quality_gate_blocks(self, mock_send):
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        stocks = [_make_stock(score=80.0)]
        quality = {"bad": 20.0}
        result = send_signal_alerts(config, stocks, channel_slug="bad", channel_quality_scores=quality)
        assert result is False
        mock_send.assert_not_called()
