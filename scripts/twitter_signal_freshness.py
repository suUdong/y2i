"""Compare Twitter-source SignalRecords against news and YouTube records.

Three-source extension of ``scripts/news_signal_freshness.py``. Answers the
two operating questions for the y2i twitter pilot:

  1. Is Twitter delivering tickers that neither news nor YouTube surfaced in
     the recent window? (unique vs overlap)
  2. For tickers that fire on multiple sources, does Twitter lead or lag
     news/YouTube in time?

Plus the same 5d hit-rate computation per source so downstream channel
weighting can compare cohorts apples-to-apples.

Output is JSON. Cron-safe: exits 0 even on an empty tracker.

Usage:
    python -m scripts.twitter_signal_freshness \
        --tracker-db .omx/state/signal_tracker.json --window-days 14
"""

from __future__ import annotations

import argparse
import json
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB

_NEWS_PREFIX = "news:"
_TWITTER_PREFIX = "twitter:"


def _classify_source(channel_slug: str) -> str:
    if channel_slug.startswith(_TWITTER_PREFIX):
        return "twitter"
    if channel_slug.startswith(_NEWS_PREFIX):
        return "news"
    return "youtube"


def _parse_signal_dt(record: SignalRecord) -> datetime | None:
    raw = (record.signal_date or "").strip()
    if not raw:
        return None
    try:
        if "T" in raw or "+" in raw or "Z" in raw:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        else:
            parsed = datetime.fromisoformat(raw[:10])
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ticker_first_signal(records: Iterable[SignalRecord]) -> dict[str, datetime]:
    earliest: dict[str, datetime] = {}
    for record in records:
        ticker = (record.ticker or "").upper()
        if not ticker:
            continue
        dt = _parse_signal_dt(record)
        if dt is None:
            continue
        prev = earliest.get(ticker)
        if prev is None or dt < prev:
            earliest[ticker] = dt
    return earliest


def _hit_rate_5d(records: Iterable[SignalRecord]) -> dict[str, float | int | None]:
    matured = 0
    wins = 0
    returns: list[float] = []
    for record in records:
        value = (record.returns or {}).get("5d")
        if value is None:
            continue
        matured += 1
        if float(value) > 0:
            wins += 1
        returns.append(float(value))
    return {
        "matured": matured,
        "win_rate_pct": round((wins / matured) * 100.0, 2) if matured else None,
        "median_return_pct": round(statistics.median(returns), 2) if returns else None,
    }


def _lead_summary(
    a_first: dict[str, datetime],
    b_first: dict[str, datetime],
    *,
    leader_label: str,
    follower_label: str,
) -> dict:
    """Return median hours by which ``a_first`` leads ``b_first`` on shared tickers."""
    overlap = set(a_first) & set(b_first)
    lead_hours: list[float] = []
    for ticker in overlap:
        delta_hours = (b_first[ticker] - a_first[ticker]).total_seconds() / 3600.0
        lead_hours.append(delta_hours)
    return {
        "leader": leader_label,
        "follower": follower_label,
        "shared_tickers": len(overlap),
        "median_hours_leader_leads_follower": (
            round(statistics.median(lead_hours), 2) if lead_hours else None
        ),
        "positive_lead_count": sum(1 for h in lead_hours if h > 0),
    }


def compute_freshness_report(
    records: list[SignalRecord],
    *,
    window_days: int = 14,
    now: datetime | None = None,
) -> dict:
    """Build the freshness/lead-time/hit-rate summary from raw records."""
    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_days * 24 * 3600

    by_source: dict[str, list[SignalRecord]] = {"twitter": [], "news": [], "youtube": []}
    for record in records:
        by_source[_classify_source(record.channel_slug or "")].append(record)

    first_by_source = {src: _ticker_first_signal(by_source[src]) for src in by_source}

    recent_by_source = {
        src: {t for t, dt in mapping.items() if dt.timestamp() >= cutoff}
        for src, mapping in first_by_source.items()
    }
    twitter_recent = recent_by_source["twitter"]
    news_recent = recent_by_source["news"]
    youtube_recent = recent_by_source["youtube"]

    overlap_with_news = twitter_recent & news_recent
    overlap_with_youtube = twitter_recent & youtube_recent
    twitter_unique = twitter_recent - (news_recent | youtube_recent)

    overlap_ratio_news = (
        len(overlap_with_news) / len(twitter_recent) * 100.0 if twitter_recent else 0.0
    )
    overlap_ratio_youtube = (
        len(overlap_with_youtube) / len(twitter_recent) * 100.0 if twitter_recent else 0.0
    )

    lead_vs_news = _lead_summary(
        first_by_source["twitter"], first_by_source["news"],
        leader_label="twitter", follower_label="news",
    )
    lead_vs_youtube = _lead_summary(
        first_by_source["twitter"], first_by_source["youtube"],
        leader_label="twitter", follower_label="youtube",
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "tickers_recent": {
            "twitter": len(twitter_recent),
            "news": len(news_recent),
            "youtube": len(youtube_recent),
        },
        "twitter_tickers_unique": len(twitter_unique),
        "overlap_ratio_pct": {
            "twitter_vs_news": round(overlap_ratio_news, 2),
            "twitter_vs_youtube": round(overlap_ratio_youtube, 2),
        },
        "lead_time_summary": {
            "twitter_vs_news": lead_vs_news,
            "twitter_vs_youtube": lead_vs_youtube,
        },
        "hit_rate_5d": {
            src: _hit_rate_5d(by_source[src]) for src in by_source
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker-db", default=".omx/state/signal_tracker.json")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument(
        "--output", default="-",
        help="Write JSON here, or '-' for stdout (default)",
    )
    args = parser.parse_args(argv)

    db = SignalTrackerDB(Path(args.tracker_db))
    report = compute_freshness_report(list(db.records), window_days=args.window_days)
    text = json.dumps(report, ensure_ascii=False, indent=2)

    if args.output == "-":
        print(text)
    else:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
