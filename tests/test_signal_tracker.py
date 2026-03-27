from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from omx_brainstorm.signal_tracker import (
    SignalRecord,
    SignalTrackerDB,
    AccuracyStats,
    record_signals_from_output,
    update_price_snapshots,
    TRACKING_WINDOWS,
)
from omx_brainstorm.backtest import HistoricalPricePoint
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
