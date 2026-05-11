"""Freshness report tests for the Reddit source pilot."""

from __future__ import annotations

from datetime import datetime, timezone

from omx_brainstorm.signal_tracker import SignalRecord
from scripts.reddit_signal_freshness import compute_freshness_report


def _record(
    ticker: str,
    channel_slug: str,
    signal_date: str,
    *,
    verdict: str = "BUY",
    return_5d: float | None = None,
) -> SignalRecord:
    return SignalRecord(
        ticker=ticker,
        company_name=ticker,
        channel_slug=channel_slug,
        signal_date=signal_date,
        signal_score=80.0,
        verdict=verdict,
        source_video_id=f"{channel_slug}:{ticker}:{signal_date}",
        source_title=f"{ticker} signal",
        returns={"1d": None, "3d": None, "5d": return_5d, "10d": None, "20d": None},
    )


def test_reddit_freshness_counts_unique_overlap_lead_time_and_directional_hit_rate() -> None:
    now = datetime(2026, 5, 11, 0, 0, tzinfo=timezone.utc)
    records = [
        _record("SMCI", "reddit:wallstreetbets", "2026-05-10T01:00:00+00:00", return_5d=5.0),
        _record("MU", "reddit:investing", "2026-05-09T01:00:00+00:00", verdict="SELL", return_5d=-3.0),
        _record("SMCI", "twitter:fintwit", "2026-05-10T05:00:00+00:00", return_5d=1.0),
        _record("005930.KS", "talkboard:naver:005930", "2026-05-10T02:00:00+00:00", return_5d=2.0),
        _record("MU", "news:marketwatch", "2026-05-09T05:00:00+00:00", return_5d=-1.0),
        _record("AVGO", "sampro", "2026-05-08", return_5d=1.5),
    ]

    report = compute_freshness_report(records, window_days=14, now=now)

    assert report["tickers_recent"]["reddit"] == 2
    assert report["reddit_tickers_unique"] == 0
    assert report["overlap_ratio_pct"]["reddit_vs_twitter"] == 50.0
    assert report["overlap_ratio_pct"]["reddit_vs_news"] == 50.0
    assert report["lead_time_summary"]["reddit_vs_twitter"]["median_hours_leader_leads_follower"] == 4.0
    assert report["lead_time_summary"]["reddit_vs_news"]["median_hours_leader_leads_follower"] == 4.0
    assert report["hit_rate_5d"]["reddit"]["matured"] == 2
    assert report["hit_rate_5d"]["reddit"]["win_rate_pct"] == 100.0
