"""Freshness metrics for talkboard source."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB
from scripts.talkboard_signal_freshness import compute_freshness_report, main


def _record(
    ticker: str,
    channel_slug: str,
    signal_date: str,
    *,
    returns_5d: float | None = None,
    verdict: str = "BUY",
) -> SignalRecord:
    return SignalRecord(
        ticker=ticker,
        company_name=ticker,
        channel_slug=channel_slug,
        signal_date=signal_date,
        signal_score=80.0,
        verdict=verdict,
        returns={"5d": returns_5d},
    )


def test_compute_freshness_report_counts_unique_overlap_and_lead_time() -> None:
    records = [
        _record("005930.KS", "talkboard:naver:005930", "2026-05-01T00:00:00+00:00", returns_5d=3.0),
        _record("005930.KS", "news:hankyung", "2026-05-02T00:00:00+00:00", returns_5d=2.0),
        _record("000660.KS", "talkboard:naver:000660", "2026-05-03T00:00:00+00:00", returns_5d=-1.0),
        _record("035420.KS", "twitter:hankyung_news", "2026-05-01T00:00:00+00:00", returns_5d=1.0),
        _record("035720.KS", "sampro", "2026-05-01", returns_5d=1.0),
    ]

    report = compute_freshness_report(
        records,
        window_days=14,
        now=datetime(2026, 5, 10, tzinfo=timezone.utc),
    )

    assert report["tickers_recent"]["talkboard"] == 2
    assert report["talkboard_tickers_unique"] == 1
    assert report["overlap_ratio_pct"]["talkboard_vs_news"] == 50.0
    lead = report["lead_time_summary"]["talkboard_vs_news"]
    assert lead["shared_tickers"] == 1
    assert lead["median_hours_leader_leads_follower"] == 24.0
    assert report["hit_rate_5d"]["talkboard"]["matured"] == 2
    assert report["hit_rate_5d"]["talkboard"]["win_rate_pct"] == 50.0


def test_lead_time_summary_ignores_stale_overlaps_outside_window() -> None:
    records = [
        _record("005930.KS", "talkboard:naver:005930", "2026-04-01T00:00:00+00:00"),
        _record("005930.KS", "news:hankyung", "2026-04-02T00:00:00+00:00"),
        _record("000660.KS", "talkboard:naver:000660", "2026-05-08T00:00:00+00:00"),
        _record("000660.KS", "news:hankyung", "2026-05-09T00:00:00+00:00"),
    ]

    report = compute_freshness_report(
        records,
        window_days=7,
        now=datetime(2026, 5, 10, tzinfo=timezone.utc),
    )

    lead = report["lead_time_summary"]["talkboard_vs_news"]
    assert lead["shared_tickers"] == 1
    assert lead["median_hours_leader_leads_follower"] == 24.0


def test_lead_time_uses_first_signal_inside_window_not_global_first() -> None:
    records = [
        _record("005930.KS", "talkboard:naver:005930", "2026-04-01T00:00:00+00:00"),
        _record("005930.KS", "talkboard:naver:005930", "2026-05-08T00:00:00+00:00"),
        _record("005930.KS", "news:hankyung", "2026-05-09T00:00:00+00:00"),
    ]

    report = compute_freshness_report(
        records,
        window_days=7,
        now=datetime(2026, 5, 10, tzinfo=timezone.utc),
    )

    lead = report["lead_time_summary"]["talkboard_vs_news"]
    assert lead["shared_tickers"] == 1
    assert lead["median_hours_leader_leads_follower"] == 24.0


def test_compute_freshness_report_handles_empty_tracker() -> None:
    report = compute_freshness_report([], now=datetime(2026, 5, 10, tzinfo=timezone.utc))

    assert report["tickers_recent"] == {
        "talkboard": 0,
        "twitter": 0,
        "news": 0,
        "youtube": 0,
    }
    assert report["talkboard_tickers_unique"] == 0


def test_talkboard_freshness_cli_writes_output(tmp_path: Path) -> None:
    db = SignalTrackerDB(tmp_path / "tracker.json")
    db.add_record(_record("005930.KS", "talkboard:naver:005930", "2026-05-01", returns_5d=2.0))
    output = tmp_path / "freshness.json"

    code = main([
        "--tracker-db", str(tmp_path / "tracker.json"),
        "--window-days", "14",
        "--output", str(output),
    ])

    assert code == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["hit_rate_5d"]["talkboard"]["matured"] == 1
