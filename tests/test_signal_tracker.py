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
            returns={"5d": 2.5, "10d": None},
        )
        d = record.to_dict()
        restored = SignalRecord.from_dict(d)
        assert restored.ticker == "005930.KS"
        assert restored.signal_score == 75.5
        assert restored.entry_date == "2026-03-20"
        assert restored.returns["5d"] == 2.5


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
                returns={"1d": None, "3d": None, "5d": ret, "10d": ret * 1.5, "20d": None},
            )
            db.add_record(r)
        stats = db.accuracy_report("itgod")
        assert stats.total_signals == 4
        assert stats.signals_with_price == 4
        assert stats.hit_rate_5d == 50.0  # 2 out of 4 positive
        assert stats.avg_return_5d == 1.25  # (3-1+5-2)/4
        assert stats.window_stats["5d"]["tracked"] == 4
        assert stats.window_stats["10d"]["hit_rate"] == 50.0
        assert stats.avg_signal_score == 70.0

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
