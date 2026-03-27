from __future__ import annotations

import pytest

from omx_brainstorm.channel_quality import (
    ChannelQualityReport,
    compute_channel_quality,
    compute_dynamic_weights,
    rank_channels,
)


def _make_channel_info(
    display_name: str = "Test",
    actionable_ratio: float = 0.5,
    overall: float = 60.0,
    ranking_pp: float = 50.0,
    ranking_spearman: float | None = 0.5,
) -> dict:
    return {
        "display_name": display_name,
        "actionable_ratio": actionable_ratio,
        "ranking_spearman": ranking_spearman,
        "quality_scorecard": {
            "overall": overall,
            "transcript_coverage": 100.0,
            "actionable_density": actionable_ratio * 120,
            "ranking_predictive_power": ranking_pp,
            "horizon_adequacy": 50.0,
        },
    }


class TestComputeChannelQuality:
    def test_basic_computation(self):
        channels = {
            "itgod": _make_channel_info("IT의 신", 0.733, 67.3, 47.6, -0.0877),
            "syuka": _make_channel_info("슈카월드", 0.267, 70.2, 73.9, 1.0),
        }
        reports = compute_channel_quality(channels)
        assert len(reports) == 2
        for r in reports:
            assert r.overall_quality_score > 0
            assert r.hit_rate_5d is None  # no accuracy data

    def test_with_accuracy_data(self):
        channels = {
            "itgod": _make_channel_info("IT의 신", 0.733, 67.3, 47.6),
        }
        accuracy = {
            "itgod": {
                "hit_rate_1d": 85.0,
                "hit_rate_3d": 82.0,
                "hit_rate_5d": 80.0,
                "hit_rate_10d": 75.0,
                "avg_return_1d": 2.0,
                "avg_return_3d": 4.0,
                "avg_return_5d": 8.0,
                "avg_return_10d": 6.0,
                "target_count": 3,
                "target_hit_rate": 66.7,
                "avg_target_progress_pct": 84.5,
                "pending_targets": 1,
            },
        }
        reports = compute_channel_quality(channels, accuracy)
        assert len(reports) == 1
        assert reports[0].hit_rate_1d == 85.0
        assert reports[0].hit_rate_3d == 82.0
        assert reports[0].hit_rate_5d == 80.0
        assert reports[0].avg_return_1d == 2.0
        assert reports[0].avg_return_3d == 4.0
        assert reports[0].avg_return_5d == 8.0
        assert reports[0].target_count == 3
        assert reports[0].target_hit_rate == 66.7
        assert reports[0].avg_target_progress_pct == 84.5
        # With real accuracy data, score should differ from no-data case
        no_acc_reports = compute_channel_quality(channels)
        # Scores will differ because accuracy bonus changes
        assert reports[0].overall_quality_score != no_acc_reports[0].overall_quality_score

    def test_prefers_directional_returns_when_available(self):
        channels = {
            "itgod": _make_channel_info("IT의 신", 0.6, 65.0, 50.0),
        }
        accuracy = {
            "itgod": {
                "hit_rate_1d": 100.0,
                "hit_rate_3d": 100.0,
                "hit_rate_5d": 100.0,
                "avg_return_1d": -1.0,
                "avg_return_3d": -2.0,
                "avg_return_5d": -4.0,
                "avg_directional_return_1d": 1.0,
                "avg_directional_return_3d": 2.0,
                "avg_directional_return_5d": 4.0,
            },
        }
        report = compute_channel_quality(channels, accuracy)[0]
        assert report.avg_return_5d == -4.0
        assert report.overall_quality_score > compute_channel_quality(channels)[0].overall_quality_score

    def test_empty_channels(self):
        reports = compute_channel_quality({})
        assert reports == []

    def test_missing_scorecard(self):
        channels = {"test": {"display_name": "Test", "actionable_ratio": 0.5}}
        reports = compute_channel_quality(channels)
        assert len(reports) == 1
        assert reports[0].ranking_predictive_power == 0


class TestRankChannels:
    def test_ranking_order(self):
        channels = {
            "low": _make_channel_info("Low", 0.1, 30.0, 10.0),
            "high": _make_channel_info("High", 0.8, 80.0, 80.0),
            "mid": _make_channel_info("Mid", 0.5, 60.0, 50.0),
        }
        reports = compute_channel_quality(channels)
        ranked = rank_channels(reports)
        assert ranked[0].slug == "high"
        assert ranked[-1].slug == "low"

    def test_stable_sort_on_tie(self):
        channels = {
            "b_channel": _make_channel_info("B", 0.5, 60.0, 50.0),
            "a_channel": _make_channel_info("A", 0.5, 60.0, 50.0),
        }
        reports = compute_channel_quality(channels)
        ranked = rank_channels(reports)
        # Equal scores, should sort by slug alphabetically
        assert ranked[0].slug == "a_channel"
        assert ranked[1].slug == "b_channel"


class TestComputeDynamicWeights:
    def _make_report(self, slug: str, quality: float) -> ChannelQualityReport:
        return ChannelQualityReport(
            slug=slug, display_name=slug, actionable_ratio=0.5,
            avg_signal_score=50.0, hit_rate_1d=50.0, hit_rate_3d=50.0,
            hit_rate_5d=50.0, hit_rate_10d=50.0,
            avg_return_1d=1.0, avg_return_3d=1.0,
            avg_return_5d=1.0, avg_return_10d=1.0, spearman_correlation=0.5,
            ranking_predictive_power=50.0, overall_quality_score=quality,
        )

    def test_empty_returns_empty(self):
        assert compute_dynamic_weights([]) == {}

    def test_single_channel_gets_neutral_weight(self):
        reports = [self._make_report("a", 70.0)]
        weights = compute_dynamic_weights(reports)
        assert 1.0 < weights["a"] <= 1.5

    def test_stronger_channel_gets_higher_weight(self):
        reports = [self._make_report("best", 90.0), self._make_report("worst", 30.0)]
        weights = compute_dynamic_weights(reports)
        assert weights["best"] > weights["worst"]
        assert weights["best"] > 1.0
        assert weights["worst"] < 1.0

    def test_low_accuracy_channel_gets_extra_penalty(self):
        low = ChannelQualityReport(
            slug="low", display_name="low", actionable_ratio=0.5,
            avg_signal_score=40.0, hit_rate_1d=20.0, hit_rate_3d=25.0,
            hit_rate_5d=30.0, hit_rate_10d=35.0,
            avg_return_1d=-3.0, avg_return_3d=-4.0,
            avg_return_5d=-5.0, avg_return_10d=-6.0,
            spearman_correlation=0.1, ranking_predictive_power=20.0,
            overall_quality_score=35.0,
        )
        high = self._make_report("high", 85.0)
        weights = compute_dynamic_weights([high, low])
        assert weights["low"] <= 0.7
        assert weights["high"] >= 1.1

    def test_sparse_accuracy_stays_near_neutral(self):
        report = ChannelQualityReport(
            slug="sparse", display_name="sparse", actionable_ratio=0.5,
            avg_signal_score=60.0, hit_rate_1d=None, hit_rate_3d=None,
            hit_rate_5d=None, hit_rate_10d=None,
            avg_return_1d=None, avg_return_3d=None,
            avg_return_5d=None, avg_return_10d=None,
            spearman_correlation=0.5, ranking_predictive_power=50.0,
            overall_quality_score=62.0,
        )
        weights = compute_dynamic_weights([report])
        assert 0.9 <= weights["sparse"] <= 1.1

    def test_sparse_high_hit_rate_does_not_outweigh_mature_winner(self):
        sparse = ChannelQualityReport(
            slug="sparse", display_name="sparse", actionable_ratio=0.6,
            avg_signal_score=72.0, hit_rate_1d=100.0, hit_rate_3d=100.0,
            hit_rate_5d=100.0, hit_rate_10d=None,
            avg_return_1d=4.0, avg_return_3d=6.0,
            avg_return_5d=8.0, avg_return_10d=None,
            spearman_correlation=0.6, ranking_predictive_power=62.0,
            overall_quality_score=74.0,
            signals_with_price_1d=1, signals_with_price_3d=1, signals_with_price_5d=1,
            avg_directional_return_1d=4.0, avg_directional_return_3d=6.0, avg_directional_return_5d=8.0,
        )
        mature = ChannelQualityReport(
            slug="mature", display_name="mature", actionable_ratio=0.55,
            avg_signal_score=68.0, hit_rate_1d=66.0, hit_rate_3d=68.0,
            hit_rate_5d=70.0, hit_rate_10d=64.0,
            avg_return_1d=1.0, avg_return_3d=2.0,
            avg_return_5d=4.5, avg_return_10d=3.5,
            spearman_correlation=0.55, ranking_predictive_power=58.0,
            overall_quality_score=71.0,
            signals_with_price_1d=12, signals_with_price_3d=11, signals_with_price_5d=10,
            avg_directional_return_1d=1.0, avg_directional_return_3d=2.0, avg_directional_return_5d=4.5,
        )
        weights = compute_dynamic_weights([sparse, mature])
        assert 0.95 <= weights["sparse"] <= 1.15
        assert weights["mature"] > weights["sparse"]

    def test_weights_are_bounded(self):
        reports = [self._make_report(f"ch{i}", 90.0 - i * 10) for i in range(6)]
        weights = compute_dynamic_weights(reports)
        assert all(0.4 <= w <= 1.5 for w in weights.values())


class TestChannelQualityReport:
    def test_to_dict(self):
        report = ChannelQualityReport(
            slug="itgod", display_name="IT의 신", actionable_ratio=0.733,
            avg_signal_score=67.3, hit_rate_1d=62.0, hit_rate_3d=61.0,
            hit_rate_5d=60.0, hit_rate_10d=55.0,
            avg_return_1d=1.0, avg_return_3d=2.5,
            avg_return_5d=3.5, avg_return_10d=2.0, spearman_correlation=0.5,
            ranking_predictive_power=47.6, overall_quality_score=65.0,
        )
        d = report.to_dict()
        assert d["slug"] == "itgod"
        assert d["overall_quality_score"] == 65.0
