"""Compare talkboard SignalRecords against Twitter, news, and YouTube.

This is the fourth-source version of the y2i freshness review. It answers:

  1. Is talkboard surfacing tickers that other sources did not?
  2. Does talkboard lead or lag news/Twitter/YouTube on overlapping tickers?
  3. What is the 5d directional hit-rate by source?

Output is JSON and the script exits 0 on an empty tracker.

Usage:
    python -m scripts.talkboard_signal_freshness \
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
_SOURCES = ("talkboard", "twitter", "news", "youtube")


def _classify_source(channel_slug: str) -> str:
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
    recent_records = []
    for record in records:
        dt = _parse_signal_dt(record)
        if dt is not None and dt.timestamp() >= cutoff:
            recent_records.append(record)
    return _ticker_first_signal(recent_records)


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
    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_days * 24 * 3600

    by_source: dict[str, list[SignalRecord]] = {src: [] for src in _SOURCES}
    for record in records:
        by_source[_classify_source(record.channel_slug or "")].append(record)

    first_by_source = {src: _ticker_first_signal(by_source[src]) for src in _SOURCES}
    recent_first_by_source = {
        src: _ticker_first_signal_since(by_source[src], cutoff)
        for src in _SOURCES
    }
    recent_by_source = {
        src: set(recent_first_by_source[src])
        for src in _SOURCES
    }

    talkboard_recent = recent_by_source["talkboard"]
    non_talkboard_recent = set().union(
        recent_by_source["twitter"],
        recent_by_source["news"],
        recent_by_source["youtube"],
    )
    talkboard_unique = talkboard_recent - non_talkboard_recent

    return {
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "tickers_recent": {src: len(recent_by_source[src]) for src in _SOURCES},
        "talkboard_tickers_unique": len(talkboard_unique),
        "overlap_ratio_pct": {
            f"talkboard_vs_{src}": (
                round(len(talkboard_recent & recent_by_source[src]) / len(talkboard_recent) * 100.0, 2)
                if talkboard_recent else 0.0
            )
            for src in ("twitter", "news", "youtube")
        },
        "lead_time_summary": {
            f"talkboard_vs_{src}": _lead_summary(
                recent_first_by_source["talkboard"],
                recent_first_by_source[src],
                leader_label="talkboard",
                follower_label=src,
            )
            for src in ("twitter", "news", "youtube")
        },
        "hit_rate_5d": {
            src: _hit_rate_5d(by_source[src]) for src in _SOURCES
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
