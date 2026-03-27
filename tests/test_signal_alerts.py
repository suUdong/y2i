from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from omx_brainstorm.app_config import NotificationConfig
from omx_brainstorm.signal_alerts import (
    filter_high_quality_signals,
    format_analysis_summary,
    format_telegram_alert,
    send_analysis_summary_alert,
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

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_weight_multiplier_lowers_threshold(self, mock_send):
        """A high weight multiplier should lower the effective quality threshold."""
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        stocks = [_make_stock(score=80.0)]
        # Channel quality is 40 (below default 50 threshold)
        quality = {"good": 40.0}
        # Without weight, would be blocked:
        assert send_signal_alerts(config, stocks, channel_slug="good", channel_quality_scores=quality) is False
        # With high weight (1.5), effective threshold = 50/1.5 = 33.3, so 40 passes:
        result = send_signal_alerts(
            config, stocks, channel_slug="good", channel_quality_scores=quality,
            weight_multipliers={"good": 1.5},
        )
        assert result is True


class TestFormatAnalysisSummary:
    def test_basic_format(self):
        new_videos = {"itgod": ["v1", "v2"], "sampro": ["v3"]}
        msg = format_analysis_summary(new_videos, trigger="new_videos")
        assert "분석 완료" in msg
        assert "3" in msg  # total videos
        assert "new_videos" in msg

    def test_with_top_signals(self):
        new_videos = {"itgod": ["v1"]}
        signals = [{"company_name": "삼성전자", "aggregate_score": 82.0, "aggregate_verdict": "STRONG_BUY"}]
        msg = format_analysis_summary(new_videos, top_signals=signals)
        assert "삼성전자" in msg
        assert "STRONG_BUY" in msg

    def test_with_channel_names(self):
        new_videos = {"itgod": ["v1"]}
        msg = format_analysis_summary(new_videos, channel_names={"itgod": "IT의 신"})
        assert "IT의 신" in msg

    def test_empty_videos_returns_header(self):
        msg = format_analysis_summary({}, trigger="daily")
        assert "0" in msg

    def test_html_escaping(self):
        new_videos = {"ch": ["v1"]}
        msg = format_analysis_summary(new_videos, channel_names={"ch": "<script>alert(1)</script>"})
        assert "<script>" not in msg
        assert "&lt;script&gt;" in msg


class TestSendAnalysisSummaryAlert:
    def test_empty_videos_returns_false(self):
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        assert send_analysis_summary_alert(config, {}) is False

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_sends_when_videos_present(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        result = send_analysis_summary_alert(config, {"itgod": ["v1"]}, trigger="new_videos")
        assert result is True
        mock_send.assert_called_once()

    def test_missing_credentials_returns_false(self):
        config = NotificationConfig()
        result = send_analysis_summary_alert(config, {"itgod": ["v1"]})
        assert result is False
