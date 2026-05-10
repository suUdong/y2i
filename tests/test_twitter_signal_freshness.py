"""Tests for scripts/twitter_signal_freshness.py — 3-source freshness report."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB
from scripts.twitter_signal_freshness import compute_freshness_report, main


def _record(
    *,
    ticker: str,
    channel_slug: str,
    signal_date: str,
    returns_5d: float | None = None,
) -> SignalRecord:
    return SignalRecord(
        ticker=ticker,
        company_name="Sample",
        channel_slug=channel_slug,
        signal_date=signal_date,
        signal_score=80.0,
        verdict="BUY",
        returns={"5d": returns_5d} if returns_5d is not None else {},
    )


def test_empty_tracker_returns_zeroed_report() -> None:
    report = compute_freshness_report([], window_days=14)
    assert report["tickers_recent"]["twitter"] == 0
    assert report["tickers_recent"]["news"] == 0
    assert report["tickers_recent"]["youtube"] == 0
    assert report["twitter_tickers_unique"] == 0
    assert report["overlap_ratio_pct"]["twitter_vs_news"] == 0.0
    assert report["overlap_ratio_pct"]["twitter_vs_youtube"] == 0.0


def test_twitter_unique_ticker_counted_when_no_other_source_mentions() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _record(ticker="005930.KS", channel_slug="twitter:hankyung_news", signal_date="2026-05-10"),
        _record(ticker="000660.KS", channel_slug="news:hankyung", signal_date="2026-05-10"),
    ]
    report = compute_freshness_report(records, window_days=14, now=now)
    assert report["twitter_tickers_unique"] == 1
    assert report["tickers_recent"]["twitter"] == 1
    assert report["tickers_recent"]["news"] == 1
    assert report["overlap_ratio_pct"]["twitter_vs_news"] == 0.0


def test_overlap_with_news_when_same_ticker_fires_on_both() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _record(ticker="005930.KS", channel_slug="twitter:hankyung_news", signal_date="2026-05-09"),
        _record(ticker="005930.KS", channel_slug="news:hankyung", signal_date="2026-05-10"),
    ]
    report = compute_freshness_report(records, window_days=14, now=now)
    assert report["overlap_ratio_pct"]["twitter_vs_news"] == 100.0
    assert report["twitter_tickers_unique"] == 0
    lead = report["lead_time_summary"]["twitter_vs_news"]
    assert lead["shared_tickers"] == 1
    assert lead["leader"] == "twitter"
    # Twitter fired one day earlier → median lead is +24h (positive).
    assert lead["median_hours_leader_leads_follower"] == 24.0
    assert lead["positive_lead_count"] == 1


def test_twitter_can_lag_news_negative_lead_time() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _record(ticker="005930.KS", channel_slug="twitter:hankyung_news", signal_date="2026-05-10"),
        _record(ticker="005930.KS", channel_slug="news:hankyung", signal_date="2026-05-09"),
    ]
    report = compute_freshness_report(records, window_days=14, now=now)
    lead = report["lead_time_summary"]["twitter_vs_news"]
    assert lead["median_hours_leader_leads_follower"] == -24.0
    assert lead["positive_lead_count"] == 0


def test_window_filter_drops_records_outside_lookback() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _record(ticker="005930.KS", channel_slug="twitter:hankyung_news", signal_date="2026-01-01"),
    ]
    report = compute_freshness_report(records, window_days=14, now=now)
    assert report["tickers_recent"]["twitter"] == 0


def test_hit_rate_per_source_uses_5d_returns() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _record(ticker="005930.KS", channel_slug="twitter:a", signal_date="2026-05-09", returns_5d=2.0),
        _record(ticker="000660.KS", channel_slug="twitter:b", signal_date="2026-05-09", returns_5d=-1.0),
        _record(ticker="042700.KS", channel_slug="news:hankyung", signal_date="2026-05-09", returns_5d=4.0),
    ]
    report = compute_freshness_report(records, window_days=14, now=now)
    twitter_hit = report["hit_rate_5d"]["twitter"]
    assert twitter_hit["matured"] == 2
    assert twitter_hit["win_rate_pct"] == 50.0  # 1 of 2 positive
    news_hit = report["hit_rate_5d"]["news"]
    assert news_hit["matured"] == 1
    assert news_hit["win_rate_pct"] == 100.0


def test_cli_writes_report_to_output_file(tmp_path: Path) -> None:
    db_path = tmp_path / "tracker.json"
    db = SignalTrackerDB(db_path=db_path)
    db.add_record(_record(ticker="005930.KS", channel_slug="twitter:a", signal_date="2026-05-10"))
    output = tmp_path / "report.json"
    rc = main([
        "--tracker-db", str(db_path),
        "--window-days", "30",
        "--output", str(output),
    ])
    assert rc == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["window_days"] == 30
    assert "twitter_tickers_unique" in payload


def test_cli_is_safe_on_empty_tracker(tmp_path: Path) -> None:
    db_path = tmp_path / "empty.json"
    SignalTrackerDB(db_path=db_path)  # writes an empty store
    rc = main([
        "--tracker-db", str(db_path),
        "--output", str(tmp_path / "out.json"),
    ])
    assert rc == 0
