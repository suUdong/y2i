"""Compare Reddit SignalRecords against talkboard, Twitter, news, and YouTube.

This is the fifth-source freshness review for the y2i Reddit pilot. It reports
recent unique tickers, overlap/lead-time against the existing four source
classes, and 5d directional hit-rate by source.

Usage:
    python -m scripts.reddit_signal_freshness \
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
_TALKBOARD_PREFIX = "talkboard:"
_REDDIT_PREFIX = "reddit:"
_SOURCES = ("reddit", "talkboard", "twitter", "news", "youtube")


def _classify_source(channel_slug: str) -> str:
    if channel_slug.startswith(_REDDIT_PREFIX):
        return "reddit"
    if channel_slug.startswith(_TALKBOARD_PREFIX):
        return "talkboard"
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


def _ticker_first_signal_since(records: Iterable[SignalRecord], cutoff: float) -> dict[str, datetime]:
    return _ticker_first_signal(
        record for record in records
        if (dt := _parse_signal_dt(record)) is not None and dt.timestamp() >= cutoff
    )


def _hit_rate_5d(records: Iterable[SignalRecord]) -> dict[str, float | int | None]:
    matured = 0
    wins = 0
    returns: list[float] = []
    for record in records:
        value = (record.returns or {}).get("5d")
        if value is None:
            continue
        matured += 1
        directional = -float(value) if str(record.verdict).upper() in {"SELL", "AVOID", "REJECT"} else float(value)
        if directional > 0:
            wins += 1
        returns.append(directional)
    return {
        "matured": matured,
        "win_rate_pct": round((wins / matured) * 100.0, 2) if matured else None,
        "median_directional_return_pct": round(statistics.median(returns), 2) if returns else None,
    }


def _lead_summary(
    a_first: dict[str, datetime],
    b_first: dict[str, datetime],
    *,
    leader_label: str,
    follower_label: str,
) -> dict:
    overlap = set(a_first) & set(b_first)
    lead_hours: list[float] = []
    for ticker in overlap:
        lead_hours.append((b_first[ticker] - a_first[ticker]).total_seconds() / 3600.0)
    return {
        "leader": leader_label,
        "follower": follower_label,
        "shared_tickers": len(overlap),
        "median_hours_leader_leads_follower": (
            round(statistics.median(lead_hours), 2) if lead_hours else None
        ),
        "positive_lead_count": sum(1 for hours in lead_hours if hours > 0),
    }


def compute_freshness_report(
    records: list[SignalRecord],
    *,
    window_days: int = 14,
    now: datetime | None = None,
) -> dict:
    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_days * 24 * 3600

    by_source: dict[str, list[SignalRecord]] = {source: [] for source in _SOURCES}
    for record in records:
        by_source[_classify_source(record.channel_slug or "")].append(record)

    recent_first_by_source = {
        source: _ticker_first_signal_since(by_source[source], cutoff)
        for source in _SOURCES
    }
    recent_by_source = {
        source: set(recent_first_by_source[source])
        for source in _SOURCES
    }
    reddit_recent = recent_by_source["reddit"]
    non_reddit_recent = set().union(
        recent_by_source["talkboard"],
        recent_by_source["twitter"],
        recent_by_source["news"],
        recent_by_source["youtube"],
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "tickers_recent": {source: len(recent_by_source[source]) for source in _SOURCES},
        "reddit_tickers_unique": len(reddit_recent - non_reddit_recent),
        "overlap_ratio_pct": {
            f"reddit_vs_{source}": (
                round(len(reddit_recent & recent_by_source[source]) / len(reddit_recent) * 100.0, 2)
                if reddit_recent else 0.0
            )
            for source in ("talkboard", "twitter", "news", "youtube")
        },
        "lead_time_summary": {
            f"reddit_vs_{source}": _lead_summary(
                recent_first_by_source["reddit"],
                recent_first_by_source[source],
                leader_label="reddit",
                follower_label=source,
            )
            for source in ("talkboard", "twitter", "news", "youtube")
        },
        "hit_rate_5d": {
            source: _hit_rate_5d(by_source[source]) for source in _SOURCES
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker-db", default=".omx/state/signal_tracker.json")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument("--output", default="-", help="Write JSON here, or '-' for stdout")
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
