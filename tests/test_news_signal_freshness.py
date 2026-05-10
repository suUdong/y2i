"""Tests for scripts/news_signal_freshness.py freshness/lead-time math."""

from __future__ import annotations

from datetime import datetime, timezone

from omx_brainstorm.signal_tracker import SignalRecord
from scripts.news_signal_freshness import compute_freshness_report


def _rec(*, ticker: str, channel_slug: str, signal_date: str, returns: dict | None = None) -> SignalRecord:
    return SignalRecord(
        ticker=ticker,
        company_name=ticker.split(".")[0],
        channel_slug=channel_slug,
        signal_date=signal_date,
        signal_score=80.0,
        verdict="BUY",
        returns=returns or {},
    )


def test_compute_freshness_report_empty_tracker_is_safe() -> None:
    report = compute_freshness_report([])
    assert report["news_tickers_total"] == 0
    assert report["overlap_ratio_pct"] == 0.0
    assert report["lead_time_summary"]["median_hours_news_leads_yt"] is None


def test_overlap_and_unique_split_for_mixed_sources() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _rec(ticker="005930.KS", channel_slug="news:hankyung", signal_date="2026-05-10"),
        _rec(ticker="000660.KS", channel_slug="news:mt", signal_date="2026-05-10"),
        _rec(ticker="005930.KS", channel_slug="sampro", signal_date="2026-05-10"),
        _rec(ticker="035720.KS", channel_slug="sampro", signal_date="2026-05-09"),
        _rec(ticker="042700.KS", channel_slug="itgod", signal_date="2026-05-08"),
    ]

    report = compute_freshness_report(records, window_days=7, now=now)

    assert report["news_tickers_total"] == 2
    # 005930 is in both → overlap of 1; 000660 unique to news.
    assert report["news_tickers_unique"] == 1
    assert report["overlap_ratio_pct"] == 50.0
    assert report["lead_time_summary"]["shared_tickers"] == 1


def test_news_hit_rate_5d_aggregates_matured_returns_only() -> None:
    now = datetime(2026, 5, 11, tzinfo=timezone.utc)
    records = [
        _rec(ticker="005930.KS", channel_slug="news:hankyung", signal_date="2026-05-05",
             returns={"5d": 3.1}),
        _rec(ticker="000660.KS", channel_slug="news:mt", signal_date="2026-05-05",
             returns={"5d": -1.2}),
        # immature — should be ignored
        _rec(ticker="042700.KS", channel_slug="news:edaily", signal_date="2026-05-09",
             returns={"5d": None}),
    ]

    report = compute_freshness_report(records, window_days=7, now=now)
    hit = report["news_hit_rate_5d"]
    assert hit["matured"] == 2
    assert hit["win_rate_pct"] == 50.0
    assert hit["median_return_pct"] is not None
