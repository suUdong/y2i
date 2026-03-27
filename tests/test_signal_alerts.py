from __future__ import annotations

from unittest.mock import patch, MagicMock

import pytest

from omx_brainstorm.app_config import NotificationConfig
from omx_brainstorm.signal_alerts import (
    build_channel_signal_summary,
    filter_consensus_signals,
    filter_high_accuracy_targets,
    filter_high_confidence_signals,
    filter_high_quality_signals,
    format_analysis_summary,
    format_consensus_telegram_alert,
    format_daily_leaderboard_summary,
    format_high_accuracy_target_alert,
    format_telegram_alert,
    send_daily_leaderboard_alert,
    send_consensus_signal_alerts,
    send_high_accuracy_target_alerts,
    send_high_confidence_signal_alerts,
    send_analysis_summary_alert,
    send_signal_alerts,
    DEFAULT_CONSENSUS_MIN_CROSS_VALIDATION,
    DEFAULT_CONSENSUS_MIN_SCORE,
    DEFAULT_HIGH_CONFIDENCE_MIN_SCORE,
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


def _make_target_record(ticker: str = "NVDA", channel_slug: str = "itgod", progress: float = 92.0, hit: bool = False) -> dict:
    return {
        "ticker": ticker,
        "company_name": "NVIDIA",
        "channel_slug": channel_slug,
        "signal_score": 86.0,
        "latest_price": 146.0,
        "price_target": {"target_price": 150.0, "currency": "USD"},
        "target_progress_pct": progress,
        "target_hit": hit,
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

    def test_custom_title_and_footer_threshold(self):
        msg = format_telegram_alert([_make_stock()], title="🚨 Y2I 고신뢰 시그널 알림", footer_threshold=82.0)
        assert "고신뢰" in msg
        assert "82" in msg


class TestBuildChannelSignalSummary:
    def test_builds_per_channel_summary(self):
        signals = [_make_stock(score=81.0)]
        summary = build_channel_signal_summary(signals, channel_slug="itgod", channel_name="IT의 신")
        assert summary["channel_slug"] == "itgod"
        assert summary["channel_name"] == "IT의 신"
        assert summary["signals"][0]["ticker"] == "005930.KS"
        assert "점수 81.0" in summary["signals"][0]["signal_summary"]


class TestFilterHighConfidenceSignals:
    def test_requires_higher_score_and_actionable_verdict(self):
        signals = [
            _make_stock(score=85.0, verdict="BUY"),
            _make_stock(ticker="WATCH", score=90.0, verdict="WATCH"),
            _make_stock(ticker="LOW", score=75.0, verdict="BUY"),
        ]
        result = filter_high_confidence_signals(signals)
        assert [item["ticker"] for item in result] == ["005930.KS"]


class TestFilterConsensusSignals:
    def test_requires_multi_channel_score_and_cross_validation(self):
        signals = [
            {**_make_stock(score=88.0), "channel_count": 2, "cross_validation_score": 81.0},
            {**_make_stock(ticker="LOW", score=74.0), "channel_count": 2, "cross_validation_score": 90.0},
            {**_make_stock(ticker="SOLO", score=91.0), "channel_count": 1, "cross_validation_score": 90.0},
        ]
        result = filter_consensus_signals(signals)
        assert [item["ticker"] for item in result] == ["005930.KS"]


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


class TestSendHighConfidenceSignalAlerts:
    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_sends_only_high_confidence_signals(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        signals = [
            _make_stock(score=85.0, verdict="STRONG_BUY"),
            _make_stock(ticker="LOW", score=76.0, verdict="BUY"),
        ]
        result = send_high_confidence_signal_alerts(config, signals, channel_name="Test")
        assert result is True
        msg = mock_send.call_args[0][1]
        assert "고신뢰" in msg
        assert f"{DEFAULT_HIGH_CONFIDENCE_MIN_SCORE:g}" in msg
        assert "LOW" not in msg

    def test_missing_credentials_returns_false(self):
        result = send_high_confidence_signal_alerts(NotificationConfig(), [_make_stock(score=90.0)])
        assert result is False


class TestHighAccuracyTargets:
    def test_filter_high_accuracy_targets(self):
        records = [
            _make_target_record(progress=88.0),
            _make_target_record(ticker="LOW", progress=60.0),
            _make_target_record(ticker="SPARSE", channel_slug="sparse", progress=95.0),
        ]
        accuracy = {
            "itgod": {"target_count": 4, "target_hit_rate": 62.0},
            "sparse": {"target_count": 1, "target_hit_rate": 100.0},
        }
        result = filter_high_accuracy_targets(records, accuracy_by_channel=accuracy)
        assert [item["ticker"] for item in result] == ["NVDA"]

    def test_format_high_accuracy_target_alert(self):
        msg = format_high_accuracy_target_alert(
            [_make_target_record(progress=100.0, hit=True)],
            channel_names={"itgod": "IT의 신"},
        )
        assert "고정확도 타겟" in msg
        assert "IT의 신" in msg
        assert "목표" in msg
        assert "달성" in msg

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_send_high_accuracy_target_alerts(self, mock_send):
        mock_send.return_value = True
        result = send_high_accuracy_target_alerts(
            NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123"),
            [_make_target_record(progress=96.0)],
            accuracy_by_channel={"itgod": {"target_count": 3, "target_hit_rate": 70.0}},
            channel_names={"itgod": "IT의 신"},
        )
        assert result is True
        assert "IT의 신" in mock_send.call_args[0][1]


class TestConsensusSignalAlerts:
    def test_format_consensus_alert(self):
        msg = format_consensus_telegram_alert([
            {
                **_make_stock(score=89.0, verdict="STRONG_BUY"),
                "channel_count": 3,
                "cross_validation_score": 84.0,
                "cross_validation_status": "CONFIRMED",
                "consensus_strength": "STRONG",
                "_source_channels_display": ["삼프로TV", "IT의 신"],
            }
        ])
        assert "합의 시그널" in msg
        assert "3개 채널" in msg
        assert "강한 합의" in msg
        assert "84.0" in msg

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_send_consensus_signal_alerts(self, mock_send):
        mock_send.return_value = True
        result = send_consensus_signal_alerts(
            NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123"),
            [
                {
                    **_make_stock(score=91.0, verdict="STRONG_BUY"),
                    "channel_count": 2,
                    "cross_validation_score": 82.0,
                    "cross_validation_status": "CONFIRMED",
                    "consensus_strength": "MODERATE",
                }
            ],
        )
        assert result is True
        msg = mock_send.call_args[0][1]
        assert f"{DEFAULT_CONSENSUS_MIN_SCORE:g}" in msg

    def test_send_consensus_signal_alerts_returns_false_when_no_match(self):
        result = send_consensus_signal_alerts(
            NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123"),
            [{**_make_stock(score=75.0), "channel_count": 2, "cross_validation_score": DEFAULT_CONSENSUS_MIN_CROSS_VALIDATION - 1}],
        )
        assert result is False


class TestFormatAnalysisSummary:
    def test_basic_format(self):
        new_videos = {"itgod": ["v1", "v2"], "sampro": ["v3"]}
        msg = format_analysis_summary(new_videos, trigger="new_videos")
        assert "분석 완료" in msg
        assert "3" in msg  # total videos
        assert "new_videos" in msg

    def test_with_top_signals(self):
        new_videos = {"itgod": ["v1"]}
        signals = [{
            "company_name": "삼성전자",
            "aggregate_score": 82.0,
            "aggregate_verdict": "STRONG_BUY",
            "channel_count": 2,
            "consensus_signal": True,
            "consensus_strength": "STRONG",
            "cross_validation_status": "CONFIRMED",
            "cross_validation_score": 84.0,
            "_source_channels_display": ["IT의 신", "삼프로TV"],
        }]
        msg = format_analysis_summary(new_videos, top_signals=signals)
        assert "삼성전자" in msg
        assert "STRONG_BUY" in msg
        assert "2개 채널 합의" in msg
        assert "강한 합의" in msg
        assert "교차검증 완료" in msg

    def test_with_channel_names(self):
        new_videos = {"itgod": ["v1"]}
        msg = format_analysis_summary(new_videos, channel_names={"itgod": "IT의 신"})
        assert "IT의 신" in msg

    def test_with_channel_signal_summaries(self):
        summaries = [
            {
                "channel_slug": "itgod",
                "channel_name": "IT의 신",
                "signals": [
                    {
                        "ticker": "NVDA",
                        "company_name": "NVIDIA",
                        "aggregate_score": 88.0,
                        "aggregate_verdict": "BUY",
                        "signal_summary": "BUY | 점수 88.0 | 언급 4회",
                    }
                ],
            }
        ]
        msg = format_analysis_summary({}, trigger="daily", channel_signal_summaries=summaries)
        assert "IT의 신" in msg
        assert "NVDA" in msg
        assert "점수 88.0" in msg

    def test_empty_videos_returns_header(self):
        msg = format_analysis_summary({}, trigger="daily")
        assert "0" in msg

    def test_html_escaping(self):
        new_videos = {"ch": ["v1"]}
        msg = format_analysis_summary(new_videos, channel_names={"ch": "<script>alert(1)</script>"})
        assert "<script>" not in msg
        assert "&lt;script&gt;" in msg


class TestSendAnalysisSummaryAlert:
    def test_empty_payload_returns_false(self):
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        assert send_analysis_summary_alert(config, {}) is False

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_sends_when_videos_present(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        result = send_analysis_summary_alert(config, {"itgod": ["v1"]}, trigger="new_videos")
        assert result is True
        mock_send.assert_called_once()

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_sends_when_channel_summaries_present_without_new_videos(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        result = send_analysis_summary_alert(
            config,
            {},
            trigger="daily",
            channel_signal_summaries=[{"channel_slug": "itgod", "channel_name": "IT의 신", "signals": []}],
        )
        assert result is True
        mock_send.assert_called_once()

    def test_missing_credentials_returns_false(self):
        config = NotificationConfig()
        result = send_analysis_summary_alert(config, {"itgod": ["v1"]})
        assert result is False


class TestDailyLeaderboardSummary:
    def test_formats_leaderboard(self):
        leaderboard = [
            {"slug": "sampro", "display_name": "삼프로TV", "overall_quality_score": 77.2, "weight_multiplier": 1.18, "hit_rate_3d": 61.0, "hit_rate_5d": 66.0, "avg_return_5d": 3.2, "actionable_ratio": 0.55},
            {"slug": "itgod", "display_name": "IT의 신", "overall_quality_score": 71.0, "actionable_ratio": 0.41},
        ]
        msg = format_daily_leaderboard_summary(leaderboard, generated_at="20260327T140000Z")
        assert "일일 채널 리더보드" in msg
        assert "삼프로TV" in msg
        assert "77.2" in msg
        assert "66.0%" in msg
        assert "1.18x" in msg
        assert "3d 적중률 61.0%" in msg

    @patch("omx_brainstorm.signal_alerts._send_telegram_html")
    def test_send_daily_leaderboard_alert(self, mock_send):
        mock_send.return_value = True
        config = NotificationConfig(telegram_bot_token="tok", telegram_chat_id="123")
        leaderboard = [{"slug": "sampro", "display_name": "삼프로TV", "overall_quality_score": 77.2}]
        assert send_daily_leaderboard_alert(config, leaderboard, generated_at="20260327T140000Z") is True
