from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from omx_brainstorm.signal_tracker import (
    SignalRecord,
    SignalTrackerDB,
    AccuracyStats,
    build_signal_backtest_summary,
    build_signal_accuracy_summary,
    record_signals_from_output,
    record_signals_from_ranking,
    record_signals_from_rows,
    save_signal_tracker_snapshot,
    save_signal_backtest_report,
    save_signal_accuracy_report,
    update_price_snapshots,
    TRACKING_WINDOWS,
)
from omx_brainstorm.backtest import HistoricalPricePoint
from omx_brainstorm.kindshot_feed import export_signals_for_kindshot
from omx_brainstorm.price_targets import aggregate_price_targets


class FakeHistoryProvider:
    def __init__(self, prices: dict[str, list[HistoricalPricePoint]] | None = None):
        self.prices = prices or {}

    def get_price_history(self, ticker: str, start_date: str, end_date: str) -> list[HistoricalPricePoint]:
        return self.prices.get(ticker, [])


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "signal_tracker.json"


@pytest.fixture
def db(db_path: Path) -> SignalTrackerDB:
    return SignalTrackerDB(db_path)


class TestSignalRecord:
    def test_roundtrip(self):
        record = SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="itgod",
            signal_date="2026-03-20",
            signal_score=75.5,
            verdict="BUY",
            entry_date="2026-03-20",
            entry_price=58000.0,
            latest_price=59200.0,
            latest_price_date="2026-03-25",
            price_path=[
                {"date": "2026-03-20", "close": 58000.0, "days_from_signal": 0, "days_from_entry": 0, "return_pct": 0.0},
                {"date": "2026-03-25", "close": 59200.0, "days_from_signal": 5, "days_from_entry": 5, "return_pct": 2.07},
            ],
            price_target={"target_price": 62000.0, "currency": "KRW"},
            target_progress_pct=57.14,
            target_distance_pct=4.73,
            returns={"5d": 2.5, "10d": None},
        )
        d = record.to_dict()
        restored = SignalRecord.from_dict(d)
        assert restored.ticker == "005930.KS"
        assert restored.signal_score == 75.5
        assert restored.entry_date == "2026-03-20"
        assert restored.returns["5d"] == 2.5
        assert restored.latest_price == 59200.0
        assert restored.price_path[-1]["return_pct"] == 2.07
        assert restored.price_target["target_price"] == 62000.0
        assert restored.target_progress_pct == 57.14

    def test_roundtrip_promotes_legacy_target_hit_date(self):
        restored = SignalRecord.from_dict(
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "channel_slug": "itgod",
                "signal_date": "2026-03-20",
                "signal_score": 80.0,
                "verdict": "BUY",
                "price_target": {"target_price": 150.0, "currency": "USD"},
                "target_hit": False,
                "target_hit_date": "2026-03-21",
            }
        )
        assert restored.target_hit is True


class TestSignalTrackerDB:
    def test_add_and_persist(self, db: SignalTrackerDB, db_path: Path):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-20", signal_score=75.0, verdict="BUY",
        )
        assert db.add_record(record) is True
        assert len(db.records) == 1

        # Reload from disk
        db2 = SignalTrackerDB(db_path)
        assert len(db2.records) == 1
        assert db2.records[0].ticker == "005930.KS"

    def test_no_duplicate(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-20", signal_score=75.0, verdict="BUY",
        )
        assert db.add_record(record) is True
        assert db.add_record(record) is False
        assert len(db.records) == 1

    def test_different_channel_same_ticker(self, db: SignalTrackerDB):
        r1 = SignalRecord(ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
                          signal_date="2026-03-20", signal_score=75.0, verdict="BUY")
        r2 = SignalRecord(ticker="005930.KS", company_name="삼성전자", channel_slug="syuka",
                          signal_date="2026-03-20", signal_score=70.0, verdict="WATCH")
        assert db.add_record(r1) is True
        assert db.add_record(r2) is True
        assert len(db.records) == 2

    def test_get_records_needing_update(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=75.0, verdict="BUY",
            returns={"1d": None, "3d": None, "5d": None, "10d": None, "20d": None},
        )
        db.add_record(record)
        needs = db.get_records_needing_update(today=date(2026, 3, 10))
        assert len(needs) == 1

    def test_no_update_needed_when_filled(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=75.0, verdict="BUY",
            entry_date="2026-03-01", entry_price=50000.0,
            returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": 5.0},
        )
        db.add_record(record)
        needs = db.get_records_needing_update(today=date(2026, 3, 25))
        assert len(needs) == 0

    def test_target_records_continue_refreshing_after_return_windows_are_filled(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=88.0, verdict="BUY",
            entry_date="2026-03-01", entry_price=100.0,
            latest_price=120.0, latest_price_date="2026-03-05",
            price_target={"target_price": 150.0, "currency": "USD"},
            returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": 5.0},
        )
        db.add_record(record)
        needs = db.get_records_needing_update(today=date(2026, 3, 25))
        assert len(needs) == 1

    def test_update_returns(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-20", signal_score=75.0, verdict="BUY",
        )
        db.add_record(record)
        assert db.update_returns("005930.KS", "itgod", "2026-03-20", {"5d": 3.5})
        assert db.records[0].returns["5d"] == 3.5

    def test_accuracy_report_empty(self, db: SignalTrackerDB):
        stats = db.accuracy_report()
        assert stats.total_signals == 0
        assert stats.hit_rate_5d is None

    def test_accuracy_report(self, db: SignalTrackerDB):
        for i, ret in enumerate([3.0, -1.0, 5.0, -2.0]):
            r = SignalRecord(
                ticker=f"00{i}.KS", company_name=f"Stock{i}", channel_slug="itgod",
                signal_date=f"2026-03-0{i+1}", signal_score=70.0, verdict="BUY",
                price_target={"target_price": 110.0 + i, "currency": "USD"},
                target_progress_pct=100.0 if ret > 0 else 40.0,
                target_hit=ret > 0,
                returns={"1d": ret / 3, "3d": ret / 2, "5d": ret, "10d": ret * 1.5, "20d": None},
            )
            db.add_record(r)
        stats = db.accuracy_report("itgod")
        assert stats.total_signals == 4
        assert stats.signals_with_price == 4
        assert stats.signals_with_price_1d == 4
        assert stats.signals_with_price_3d == 4
        assert stats.signals_with_price_5d == 4
        assert stats.hit_rate_1d == 50.0
        assert stats.hit_rate_3d == 50.0
        assert stats.hit_rate_5d == 50.0  # 2 out of 4 positive
        assert stats.avg_return_1d == 0.42
        assert stats.avg_return_3d == 0.62
        assert stats.avg_return_5d == 1.25  # (3-1+5-2)/4
        assert stats.window_stats["5d"]["tracked"] == 4
        assert stats.window_stats["10d"]["hit_rate"] == 50.0
        assert stats.avg_signal_score == 70.0
        assert stats.target_count == 4
        assert stats.target_hits == 2
        assert stats.target_hit_rate == 50.0
        assert stats.pending_targets == 2

    def test_accuracy_report_channel_filter(self, db: SignalTrackerDB):
        r1 = SignalRecord(ticker="001.KS", company_name="A", channel_slug="itgod",
                          signal_date="2026-03-01", signal_score=70.0, verdict="BUY",
                          returns={"5d": 5.0, "10d": None})
        r2 = SignalRecord(ticker="002.KS", company_name="B", channel_slug="syuka",
                          signal_date="2026-03-01", signal_score=70.0, verdict="BUY",
                          returns={"5d": -3.0, "10d": None})
        db.add_record(r1)
        db.add_record(r2)
        itgod_stats = db.accuracy_report("itgod")
        assert itgod_stats.total_signals == 1
        assert itgod_stats.hit_rate_5d == 100.0

    def test_accuracy_report_handles_sparse_short_windows(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="001.KS", company_name="A", channel_slug="itgod",
                signal_date="2026-03-01", signal_score=70.0, verdict="BUY",
                returns={"1d": 1.0, "3d": None, "5d": None, "10d": None, "20d": None},
            )
        )
        stats = db.accuracy_report("itgod")
        assert stats.hit_rate_1d == 100.0
        assert stats.hit_rate_3d is None
        assert stats.avg_return_3d is None
        assert stats.signals_with_price_3d == 0

    def test_accuracy_report_treats_legacy_target_hit_date_as_hit(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord.from_dict(
                {
                    "ticker": "NVDA",
                    "company_name": "NVIDIA",
                    "channel_slug": "itgod",
                    "signal_date": "2026-03-01",
                    "signal_score": 88.0,
                    "verdict": "BUY",
                    "price_target": {"target_price": 150.0, "currency": "USD"},
                    "target_hit": False,
                    "target_hit_date": "2026-03-08",
                }
            )
        )
        stats = db.accuracy_report("itgod")
        assert stats.target_count == 1
        assert stats.target_hits == 1
        assert stats.pending_targets == 0
        assert stats.target_hit_rate == 100.0

    def test_accuracy_report_respects_bearish_verdict_direction(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="TSLA", company_name="Tesla", channel_slug="itgod",
                signal_date="2026-03-01", signal_score=82.0, verdict="SELL",
                returns={"1d": -1.0, "3d": -2.0, "5d": -4.0, "10d": -6.0, "20d": None},
            )
        )
        stats = db.accuracy_report("itgod")
        assert stats.hit_rate_1d == 100.0
        assert stats.hit_rate_5d == 100.0
        assert stats.avg_return_5d == -4.0
        assert stats.avg_directional_return_5d == 4.0

    def test_ticker_accuracy_summary_groups_across_channels(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
                signal_date="2026-03-01", signal_score=90.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 2.0, "5d": 5.0, "10d": 7.0, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="NVDA", company_name="NVIDIA", channel_slug="sampro",
                signal_date="2026-03-03", signal_score=75.0, verdict="SELL",
                returns={"1d": -1.0, "3d": -3.0, "5d": -2.0, "10d": -4.0, "20d": None},
            )
        )
        summary = db.ticker_accuracy_summary()
        assert summary[0]["ticker"] == "NVDA"
        assert summary[0]["channel_count"] == 2
        assert summary[0]["bullish_signals"] == 1
        assert summary[0]["bearish_signals"] == 1
        assert summary[0]["hit_rate_5d"] == 100.0
        assert summary[0]["avg_directional_return_5d"] == 3.5

    def test_build_and_save_signal_accuracy_report(self, db: SignalTrackerDB, tmp_path: Path):
        db.add_record(
            SignalRecord(
                ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
                signal_date="2026-03-01", signal_score=70.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": None},
            )
        )
        summary = build_signal_accuracy_summary(
            db,
            channel_metadata={"itgod": {"display_name": "IT의 신", "actionable_ratio": 0.5, "quality_scorecard": {"overall": 60.0}}},
            top_tickers=10,
        )
        json_path, txt_path = save_signal_accuracy_report(summary, tmp_path, "20260327T220000Z")
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        text = txt_path.read_text(encoding="utf-8")
        assert payload["overall"]["total_signals"] == 1
        assert "005930.KS" in payload["by_ticker"]
        assert payload["ticker_leaderboard"][0]["ticker"] == "005930.KS"
        assert "종목 리더보드" in text
        assert "삼성전자" in text

    def test_build_signal_accuracy_summary_measures_consensus_cohorts(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="NVDA", company_name="NVIDIA", channel_slug="sampro",
                signal_date="2026-03-01", signal_score=88.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 2.5, "5d": 4.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
                signal_date="2026-03-03", signal_score=84.0, verdict="BUY",
                returns={"1d": 0.8, "3d": 2.0, "5d": 3.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="TSLA", company_name="Tesla", channel_slug="sampro",
                signal_date="2026-03-05", signal_score=86.0, verdict="BUY",
                returns={"1d": -1.2, "3d": -2.5, "5d": -5.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="TSLA", company_name="Tesla", channel_slug="itgod",
                signal_date="2026-03-07", signal_score=82.0, verdict="BUY",
                returns={"1d": -0.7, "3d": -1.5, "5d": -3.0, "10d": None, "20d": None},
            )
        )

        summary = build_signal_accuracy_summary(
            db,
            channel_metadata={
                "sampro": {"display_name": "삼프로TV", "actionable_ratio": 0.6, "quality_scorecard": {"overall": 72.0}},
                "itgod": {"display_name": "IT의 신", "actionable_ratio": 0.55, "quality_scorecard": {"overall": 69.0}},
            },
        )

        consensus = summary["consensus_accuracy"]
        assert consensus["candidate_cohorts"] == 2
        assert consensus["qualified_signals"] == 0
        assert consensus["overall"]["total_signals"] == 2
        assert consensus["overall"]["hit_rate_5d"] == 50.0
        assert consensus["overall"]["avg_directional_return_5d"] == -0.25
        assert consensus["overall"]["compounded_directional_roi_5d"] == pytest.approx(-0.64, abs=0.01)
        assert consensus["recent_signals"][0]["channel_count"] == 2
        assert consensus["recent_signals"][0]["consensus_signal"] is False
        assert consensus["recent_signals"][0]["cross_validation_score"] >= 70.0
        assert consensus["recent_signals"][0]["channel_weight_sum"] < 2.15
        assert consensus["recent_signals"][0]["returns"]["5d"] == -4.0
        assert consensus["recent_signals"][1]["ticker"] == "NVDA"

    def test_build_signal_backtest_summary_filters_by_lookback(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="OLD", company_name="Old", channel_slug="itgod",
                signal_date="2025-12-01", signal_score=60.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="NEW", company_name="New", channel_slug="itgod",
                signal_date="2026-03-20", signal_score=78.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 1.5, "5d": 2.0, "10d": None, "20d": None},
            )
        )
        summary = build_signal_backtest_summary(db, lookback_days=30, as_of=date(2026, 3, 28))
        assert summary["overall"]["total_signals"] == 1
        assert summary["signals"][0]["ticker"] == "NEW"
        assert summary["start_date"] == "2026-02-27"

    def test_build_signal_backtest_summary_ranks_channels_by_5d_roi(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="A", company_name="A", channel_slug="itgod",
                signal_date="2026-03-20", signal_score=82.0, verdict="BUY",
                returns={"1d": 2.0, "3d": 3.0, "5d": 4.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="B", company_name="B", channel_slug="sampro",
                signal_date="2026-03-21", signal_score=75.0, verdict="BUY",
                returns={"1d": -1.0, "3d": 0.5, "5d": 1.0, "10d": None, "20d": None},
            )
        )
        summary = build_signal_backtest_summary(
            db,
            lookback_days=30,
            as_of=date(2026, 3, 28),
            channel_metadata={
                "itgod": {"display_name": "IT의 신", "overall_quality_score": 66.0},
                "sampro": {"display_name": "삼프로TV", "overall_quality_score": 55.0},
            },
        )
        leaderboard = summary["channel_roi_leaderboard"]
        assert leaderboard[0]["slug"] == "itgod"
        assert leaderboard[0]["compounded_directional_roi_5d"] == 4.0
        assert leaderboard[1]["slug"] == "sampro"

    def test_build_signal_backtest_summary_recommends_filters(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="HIGH1", company_name="High1", channel_slug="itgod",
                signal_date="2026-03-20", signal_score=82.0, verdict="BUY",
                price_target={"target_price": 120.0, "currency": "USD"},
                returns={"1d": 1.0, "3d": 2.0, "5d": 6.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="HIGH2", company_name="High2", channel_slug="itgod",
                signal_date="2026-03-21", signal_score=85.0, verdict="SELL",
                price_target={"target_price": 90.0, "currency": "USD"},
                returns={"1d": -1.0, "3d": -2.0, "5d": -4.0, "10d": None, "20d": None},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="LOW", company_name="Low", channel_slug="sampro",
                signal_date="2026-03-22", signal_score=60.0, verdict="WATCH",
                returns={"1d": -0.5, "3d": -1.0, "5d": -2.0, "10d": None, "20d": None},
            )
        )
        summary = build_signal_backtest_summary(
            db,
            lookback_days=30,
            as_of=date(2026, 3, 28),
            channel_metadata={
                "itgod": {"display_name": "IT의 신", "overall_quality_score": 70.0},
                "sampro": {"display_name": "삼프로TV", "overall_quality_score": 45.0},
            },
            top_filters=5,
            min_filter_sample=1,
        )
        labels = [item["label"] for item in summary["filter_recommendations"]]
        assert any("channel_quality>=55" in label for label in labels)
        assert summary["filter_recommendations"][0]["signals_with_price_5d"] >= 1

    def test_save_signal_backtest_report(self, db: SignalTrackerDB, tmp_path: Path):
        db.add_record(
            SignalRecord(
                ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
                signal_date="2026-03-20", signal_score=80.0, verdict="BUY",
                returns={"1d": 1.0, "3d": 2.0, "5d": 4.0, "10d": None, "20d": None},
            )
        )
        summary = build_signal_backtest_summary(
            db,
            lookback_days=30,
            as_of=date(2026, 3, 28),
            channel_metadata={"itgod": {"display_name": "IT의 신", "overall_quality_score": 65.0}},
            min_filter_sample=1,
        )
        json_path, txt_path = save_signal_backtest_report(summary, tmp_path, "20260327T230000Z")
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        text = txt_path.read_text(encoding="utf-8")
        assert payload["overall"]["total_signals"] == 1
        assert payload["channel_roi_leaderboard"][0]["slug"] == "itgod"
        assert "채널 ROI 리포트" in text
        assert "최적 필터 조건" in text

    def test_recent_records_target_only_filters_before_limit(self, db: SignalTrackerDB):
        db.add_record(
            SignalRecord(
                ticker="NO_TARGET", company_name="NoTarget", channel_slug="itgod",
                signal_date="2026-03-03", signal_score=80.0, verdict="BUY",
            )
        )
        db.add_record(
            SignalRecord(
                ticker="WITH_TARGET", company_name="WithTarget", channel_slug="itgod",
                signal_date="2026-03-02", signal_score=82.0, verdict="BUY",
                price_target={"target_price": 150.0, "currency": "USD"},
            )
        )
        db.add_record(
            SignalRecord(
                ticker="OLDER_TARGET", company_name="OlderTarget", channel_slug="itgod",
                signal_date="2026-03-01", signal_score=79.0, verdict="BUY",
                price_target={"target_price": 120.0, "currency": "USD"},
            )
        )
        recent = db.recent_records(limit=2, target_only=True)
        assert [item["ticker"] for item in recent] == ["WITH_TARGET", "OLDER_TARGET"]


def test_aggregate_price_targets_preserves_bearish_direction_for_hit_detection():
    summary = aggregate_price_targets(
        [{"target_price": 90.0, "currency": "USD", "direction": "DOWN"}],
        latest_price=85.0,
        currency="USD",
    )
    assert summary["direction"] == "DOWN"
    assert summary["status"] == "HIT"
    assert summary["current_vs_target_pct"] == 0.0


class TestRecordSignalsFromOutput:
    def test_ingest_per_video_rows_payload(self, db: SignalTrackerDB):
        provider = FakeHistoryProvider(
            {
                "NVDA": [HistoricalPricePoint(date="2026-03-15", close=101.0)],
                "AAPL": [HistoricalPricePoint(date="2026-03-16", close=202.0)],
            }
        )
        added = record_signals_from_rows(
            db,
            "itgod",
            [
                {
                    "video_id": "abc123",
                    "title": "엔비디아 분석",
                    "published_at": "20260315",
                    "signal_score": 76.0,
                    "stocks": [
                        {"ticker": "NVDA", "company_name": "NVIDIA", "signal_strength_score": 88.0, "final_verdict": "BUY"},
                        {"ticker": "AAPL", "company_name": "Apple", "signal_strength_score": 73.0, "final_verdict": "WATCH"},
                    ],
                }
            ],
            history_provider=provider,
        )
        assert added == 2
        assert db.records[0].source_video_id == "abc123"
        assert db.records[0].signal_date == "2026-03-15"

    def test_ingest_channel_ranking_payload(self, db: SignalTrackerDB):
        provider = FakeHistoryProvider(
            {"NVDA": [HistoricalPricePoint(date="2026-03-15", close=101.0)]}
        )
        added = record_signals_from_ranking(
            db,
            "itgod",
            [
                {
                    "ticker": "NVDA",
                    "company_name": "NVIDIA",
                    "aggregate_score": 80.0,
                    "aggregate_verdict": "BUY",
                    "first_signal_at": "2026-03-15",
                }
            ],
            history_provider=provider,
        )
        assert added == 1
        assert db.records[0].ticker == "NVDA"
        assert db.records[0].entry_price == 101.0

    def test_ingest_channel_output(self, db: SignalTrackerDB, tmp_path: Path):
        output_data = {
            "channel_slug": "itgod",
            "cross_video_ranking": [
                {
                    "ticker": "005930.KS",
                    "company_name": "삼성전자",
                    "aggregate_score": 78.5,
                    "aggregate_verdict": "BUY",
                    "first_signal_at": "2026-03-15",
                    "latest_price": 58000.0,
                    "price_target": {"target_price": 65000.0, "currency": "KRW"},
                },
                {
                    "ticker": "000660.KS",
                    "company_name": "SK하이닉스",
                    "aggregate_score": 65.0,
                    "aggregate_verdict": "WATCH",
                    "first_signal_at": "2026-03-18",
                    "latest_price": 120000.0,
                },
            ],
        }
        output_file = tmp_path / "itgod_30d_test.json"
        output_file.write_text(json.dumps(output_data), encoding="utf-8")
        provider = FakeHistoryProvider(
            {
                "005930.KS": [
                    HistoricalPricePoint(date="2026-03-15", close=57500.0),
                    HistoricalPricePoint(date="2026-03-16", close=58000.0),
                ],
                "000660.KS": [
                    HistoricalPricePoint(date="2026-03-18", close=119500.0),
                    HistoricalPricePoint(date="2026-03-19", close=120500.0),
                ],
            }
        )

        added = record_signals_from_output(db, output_file, history_provider=provider)
        assert added == 2
        assert len(db.records) == 2
        assert db.records[0].ticker == "005930.KS"
        assert db.records[0].entry_date == "2026-03-15"
        assert db.records[0].entry_price == 57500.0
        assert db.records[0].price_target["target_price"] == 65000.0

    def test_no_duplicate_ingest(self, db: SignalTrackerDB, tmp_path: Path):
        output_data = {
            "channel_slug": "itgod",
            "cross_video_ranking": [
                {"ticker": "005930.KS", "company_name": "삼성전자", "aggregate_score": 78.5,
                 "aggregate_verdict": "BUY", "first_signal_at": "2026-03-15", "latest_price": 58000.0},
            ],
        }
        output_file = tmp_path / "itgod_30d_test.json"
        output_file.write_text(json.dumps(output_data), encoding="utf-8")
        provider = FakeHistoryProvider(
            {"005930.KS": [HistoricalPricePoint(date="2026-03-15", close=57500.0)]}
        )

        assert record_signals_from_output(db, output_file, history_provider=provider) == 1
        assert record_signals_from_output(db, output_file, history_provider=provider) == 0


class TestUpdatePriceSnapshots:
    def test_update_with_price_data(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=75.0, verdict="BUY",
            entry_price=50000.0,
            returns={"1d": None, "3d": None, "5d": None, "10d": None, "20d": None},
        )
        db.add_record(record)

        prices = {
            "005930.KS": [
                HistoricalPricePoint(date="2026-03-01", close=50000.0),
                HistoricalPricePoint(date="2026-03-02", close=50500.0),
                HistoricalPricePoint(date="2026-03-04", close=51000.0),
                HistoricalPricePoint(date="2026-03-06", close=51500.0),
                HistoricalPricePoint(date="2026-03-11", close=52000.0),
                HistoricalPricePoint(date="2026-03-21", close=53000.0),
            ],
        }
        provider = FakeHistoryProvider(prices)

        # Simulate today = March 25 (all windows should be fillable)
        import omx_brainstorm.signal_tracker as mod
        original_today = date.today
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        r = db.records[0]
        assert r.entry_date == "2026-03-01"
        assert r.returns["1d"] == 1.0  # (50500-50000)/50000*100
        assert r.latest_price == 53000.0
        assert r.latest_price_date == "2026-03-21"
        assert r.price_path[0]["date"] == "2026-03-01"
        assert r.price_path[-1]["return_pct"] == 6.0

    def test_update_price_snapshots_tracks_target_progress(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=88.0, verdict="BUY",
            entry_price=100.0,
            price_target={"target_price": 150.0, "currency": "USD"},
            returns={"1d": None, "3d": None, "5d": None, "10d": None, "20d": None},
        )
        db.add_record(record)
        provider = FakeHistoryProvider(
            {
                "NVDA": [
                    HistoricalPricePoint(date="2026-03-01", close=100.0),
                    HistoricalPricePoint(date="2026-03-05", close=125.0),
                    HistoricalPricePoint(date="2026-03-08", close=150.0),
                ]
            }
        )

        import omx_brainstorm.signal_tracker as mod
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        tracked = db.records[0]
        assert tracked.target_progress_pct == 100.0
        assert tracked.target_hit is True
        assert tracked.target_hit_date == "2026-03-08"

    def test_update_price_snapshots_refreshes_target_even_after_returns_are_full(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=88.0, verdict="BUY",
            entry_date="2026-03-01", entry_price=100.0,
            price_target={"target_price": 150.0, "currency": "USD"},
            returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": 5.0},
        )
        db.add_record(record)
        provider = FakeHistoryProvider(
            {
                "NVDA": [
                    HistoricalPricePoint(date="2026-03-01", close=100.0),
                    HistoricalPricePoint(date="2026-03-08", close=150.0),
                ]
            }
        )

        import omx_brainstorm.signal_tracker as mod
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        tracked = db.records[0]
        assert tracked.latest_price == 150.0
        assert tracked.target_hit is True

    def test_update_price_snapshots_clamps_bearish_target_metrics(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=70.0, verdict="WATCH",
            entry_price=100.0,
            price_target={"target_price": 90.0, "currency": "USD"},
            returns={"1d": None, "3d": None, "5d": None, "10d": None, "20d": None},
        )
        db.add_record(record)
        provider = FakeHistoryProvider(
            {
                "NVDA": [
                    HistoricalPricePoint(date="2026-03-01", close=100.0),
                    HistoricalPricePoint(date="2026-03-08", close=85.0),
                ]
            }
        )

        import omx_brainstorm.signal_tracker as mod
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        tracked = db.records[0]
        assert tracked.target_hit is True
        assert tracked.target_progress_pct == 100.0
        assert tracked.target_distance_pct == 0.0

    def test_update_price_snapshots_preserves_ever_hit_state_after_pullback(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=90.0, verdict="BUY",
            entry_price=100.0,
            price_target={"target_price": 150.0, "currency": "USD"},
            target_hit=True,
            target_hit_date="2026-03-08",
            returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": 5.0},
        )
        db.add_record(record)
        provider = FakeHistoryProvider(
            {
                "NVDA": [
                    HistoricalPricePoint(date="2026-03-01", close=100.0),
                    HistoricalPricePoint(date="2026-03-08", close=150.0),
                    HistoricalPricePoint(date="2026-03-10", close=145.0),
                ]
            }
        )

        import omx_brainstorm.signal_tracker as mod
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        tracked = db.records[0]
        assert tracked.latest_price == 145.0
        assert tracked.target_hit is True
        assert tracked.target_progress_pct == 100.0
        assert tracked.target_distance_pct == 0.0
        assert tracked.target_hit_date == "2026-03-08"

    def test_update_price_snapshots_preserves_legacy_hit_date_only_state(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="NVDA", company_name="NVIDIA", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=90.0, verdict="BUY",
            entry_price=100.0,
            price_target={"target_price": 150.0, "currency": "USD"},
            target_hit=False,
            target_hit_date="2026-03-08",
            returns={"1d": 1.0, "3d": 2.0, "5d": 3.0, "10d": 4.0, "20d": 5.0},
        )
        db.add_record(record)
        provider = FakeHistoryProvider(
            {
                "NVDA": [
                    HistoricalPricePoint(date="2026-03-01", close=100.0),
                    HistoricalPricePoint(date="2026-03-08", close=150.0),
                    HistoricalPricePoint(date="2026-03-10", close=145.0),
                ]
            }
        )

        import omx_brainstorm.signal_tracker as mod
        try:
            mod.date = type("MockDate", (), {"today": staticmethod(lambda: date(2026, 3, 25)), "fromisoformat": date.fromisoformat})()
            updated = update_price_snapshots(db, history_provider=provider)
        finally:
            mod.date = date

        assert updated == 1
        tracked = db.records[0]
        assert tracked.target_hit is True
        assert tracked.target_progress_pct == 100.0
        assert tracked.target_hit_date == "2026-03-08"

    def test_skip_no_entry_price(self, db: SignalTrackerDB):
        record = SignalRecord(
            ticker="005930.KS", company_name="삼성전자", channel_slug="itgod",
            signal_date="2026-03-01", signal_score=75.0, verdict="BUY",
            entry_price=None,
        )
        db.add_record(record)
        provider = FakeHistoryProvider()
        updated = update_price_snapshots(db, history_provider=provider)
        assert updated == 0


def test_export_signals_for_kindshot_filters_to_kr_buy_signals(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="sampro",
            signal_date="2026-03-20",
            signal_score=88.0,
            verdict="STRONG_BUY",
            source_title="삼성전자 집중 분석",
            price_target={"target_price": 70000, "currency": "KRW"},
        )
    )
    db.add_record(
        SignalRecord(
            ticker="NVDA",
            company_name="NVIDIA",
            channel_slug="itgod",
            signal_date="2026-03-20",
            signal_score=91.0,
            verdict="BUY",
            source_title="NVDA 분석",
        )
    )
    db.add_record(
        SignalRecord(
            ticker="035420.KS",
            company_name="NAVER",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=64.0,
            verdict="BUY",
            source_title="네이버 단기 반등",
        )
    )
    db.add_record(
        SignalRecord(
            ticker="035720.KS",
            company_name="Kakao",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=76.0,
            verdict="BUY",
            source_title="카카오 점검",
        )
    )
    db.add_record(
        SignalRecord(
            ticker="012450.KS",
            company_name="Hanwha Aerospace",
            channel_slug="hsacademy",
            signal_date="2026-03-22",
            signal_score=81.0,
            verdict="STRONG_BUY",
            source_title="방산 강세 지속",
        )
    )
    db.add_record(
        SignalRecord(
            ticker="000660.KS",
            company_name="SK hynix",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=76.0,
            verdict="WATCH",
            source_title="하이닉스 점검",
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    payload = export_signals_for_kindshot(db, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["path"] == str(output_path)
    assert payload["signal_count"] == 3
    exported_tickers = [item["ticker"] for item in written["signals"]]
    assert exported_tickers == ["012450.KS", "035720.KS", "005930.KS"]
    assert written["signals"][0]["signal_source"] == "y2i"
    assert "점수" in written["signals"][0]["evidence"][0]
    assert written["signals"][2]["channel"] == "sampro"
    assert "목표가 70000 KRW" in written["signals"][2]["evidence"]


def test_save_signal_tracker_snapshot_preserves_kindshot_contract(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / ".omx" / "state" / "signal_tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=61.0,
            verdict="WATCH",
            source_video_id="vid-1",
            source_title="삼성전자 점검",
            entry_date="2026-03-21",
            entry_price=58000.0,
            returns={"1d": 0.5, "3d": 1.2, "5d": None, "10d": None, "20d": None},
        )
    )

    output_path = tmp_path / "output" / "signal_tracker.json"
    payload = save_signal_tracker_snapshot(db, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["path"] == str(output_path)
    assert payload["signal_count"] == 1
    assert "updated_at" in written
    assert list(written) == ["signals", "updated_at"]
    assert written["signals"][0]["ticker"] == "005930.KS"
    assert written["signals"][0]["company_name"] == "삼성전자"
    assert written["signals"][0]["channel_slug"] == "itgod"
    assert written["signals"][0]["signal_date"] == "2026-03-21"
    assert written["signals"][0]["signal_score"] == 61.0
    assert written["signals"][0]["verdict"] == "WATCH"


def test_export_signals_for_kindshot_excludes_negative_mature_signals(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="sampro",
            signal_date="2026-03-20",
            signal_score=91.0,
            verdict="STRONG_BUY",
            source_title="삼성전자 강한 매수",
            returns={"1d": 1.2, "3d": 2.5, "5d": 6.0, "10d": None, "20d": None},
        )
    )
    db.add_record(
        SignalRecord(
            ticker="035420.KS",
            company_name="NAVER",
            channel_slug="itgod",
            signal_date="2026-03-20",
            signal_score=92.0,
            verdict="STRONG_BUY",
            source_title="네이버 강한 매수",
            returns={"1d": -1.0, "3d": -2.5, "5d": -7.0, "10d": None, "20d": None},
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert [item["ticker"] for item in written["signals"]] == ["005930.KS"]
    assert any("5d 방향수익률 6.00%" in evidence for evidence in written["signals"][0]["evidence"])


def test_export_signals_for_kindshot_includes_consensus_metadata(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="sampro",
            signal_date="2026-03-20",
            signal_score=90.0,
            verdict="STRONG_BUY",
            source_title="삼성전자 핵심 논리",
            price_target={"target_price": 70000, "currency": "KRW"},
        )
    )
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=84.0,
            verdict="BUY",
            source_title="삼성전자 추세 확인",
            price_target={"target_price": 69000, "currency": "KRW"},
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path, channel_weights={"sampro": 1.2, "itgod": 1.05})
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert written["signals"][0]["ticker"] == "005930.KS"
    assert written["signals"][0]["consensus_signal"] is True
    assert written["signals"][0]["consensus_strength"] in {"MODERATE", "STRONG"}
    assert written["signals"][0]["consensus_channel_count"] == 2
    assert any("합의 통과" in evidence for evidence in written["signals"][0]["evidence"])


def test_export_signals_for_kindshot_skips_low_weight_single_channel_signal(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="035420.KS",
            company_name="NAVER",
            channel_slug="lowconviction",
            signal_date="2026-03-21",
            signal_score=88.0,
            verdict="BUY",
            source_title="네이버 단일 매수",
            price_target={"target_price": 250000, "currency": "KRW"},
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    payload = export_signals_for_kindshot(db, output_path, channel_weights={"lowconviction": 0.75})
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert payload["signal_count"] == 0
    assert written["signals"] == []


def test_export_signals_for_kindshot_penalizes_low_weight_channels(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    for idx, score in enumerate((88.0, 90.0, 87.0), start=1):
        db.add_record(
            SignalRecord(
                ticker=f"GOOD{idx:02d}.KS",
                company_name=f"Good {idx}",
                channel_slug="sampro",
                signal_date=f"2026-03-0{idx}",
                signal_score=score,
                verdict="BUY",
                price_target={"target_price": 70000 + idx * 1000, "currency": "KRW"},
                returns={"1d": 1.2, "3d": 2.8, "5d": 5.5, "10d": None, "20d": None},
            )
        )
    for idx, score in enumerate((82.0, 79.0, 81.0), start=1):
        db.add_record(
            SignalRecord(
                ticker=f"BAD{idx:02d}.KS",
                company_name=f"Bad {idx}",
                channel_slug="macroview",
                signal_date=f"2026-03-1{idx}",
                signal_score=score,
                verdict="BUY",
                price_target={"target_price": 62000 + idx * 500, "currency": "KRW"},
                returns={"1d": -1.0, "3d": -2.0, "5d": -4.5, "10d": None, "20d": None},
            )
        )
    db.add_record(
        SignalRecord(
            ticker="078930.KS",
            company_name="GS",
            channel_slug="macroview",
            signal_date="2026-03-21",
            signal_score=68.0,
            verdict="BUY",
            price_target={"target_price": 65000, "currency": "KRW"},
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert "078930.KS" not in [item["ticker"] for item in written["signals"]]


def test_export_signals_for_kindshot_adds_consensus_evidence(tmp_path: Path):
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="sampro",
            signal_date="2026-03-20",
            signal_score=84.0,
            verdict="BUY",
            source_title="삼성전자 비중 확대",
            price_target={"target_price": 72000, "currency": "KRW"},
            returns={"1d": 1.0, "3d": 2.2, "5d": 4.1, "10d": None, "20d": None},
        )
    )
    db.add_record(
        SignalRecord(
            ticker="005930.KS",
            company_name="삼성전자",
            channel_slug="itgod",
            signal_date="2026-03-21",
            signal_score=82.0,
            verdict="STRONG_BUY",
            source_title="삼성전자 업사이드 재평가",
            price_target={"target_price": 74000, "currency": "KRW"},
            returns={"1d": 0.8, "3d": 2.0, "5d": 3.8, "10d": None, "20d": None},
        )
    )

    output_path = tmp_path / "kindshot_feed.json"
    export_signals_for_kindshot(db, output_path)
    written = json.loads(output_path.read_text(encoding="utf-8"))

    assert [item["ticker"] for item in written["signals"]] == ["005930.KS", "005930.KS"]
    assert any("합의" in evidence for evidence in written["signals"][0]["evidence"])
    assert written["signals"][0]["confidence"] > 0.84
