"""Compare news-source SignalRecords against YouTube-source SignalRecords.

Answers two operating questions for the y2i news pilot:

  1. Is the news source delivering *fresh* tickers — i.e. ones the YouTube
     channels did not surface in the recent window?
  2. Are news signals leading the YouTube signals in time when both fire on the
     same ticker?

The output is a JSON report that downstream dashboards can ingest; the script
also exits 0 even on an empty tracker so it is cron-safe.

Usage:
    python -m scripts.news_signal_freshness --tracker-db .omx/state/signal_tracker.json --window-days 7
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


def _is_news(record: SignalRecord) -> bool:
    return (record.channel_slug or "").startswith(_NEWS_PREFIX)


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


def compute_freshness_report(
    records: list[SignalRecord],
    *,
    window_days: int = 7,
    now: datetime | None = None,
) -> dict:
    """Build the freshness/lead-time/hit-rate summary from raw records."""
    if not records:
        return {
            "generated_at": (now or datetime.now(timezone.utc)).isoformat(),
            "window_days": window_days,
            "news_tickers_total": 0,
            "news_tickers_unique": 0,
            "youtube_tickers_total": 0,
            "overlap_ratio_pct": 0.0,
            "lead_time_summary": {"shared_tickers": 0, "median_hours_news_leads_yt": None},
            "news_hit_rate_5d": _hit_rate_5d([]),
        }

    now = now or datetime.now(timezone.utc)
    cutoff = now.timestamp() - window_days * 24 * 3600

    news_records = [r for r in records if _is_news(r)]
    yt_records = [r for r in records if not _is_news(r)]

    news_first = _ticker_first_signal(news_records)
    yt_first = _ticker_first_signal(yt_records)

    news_recent_tickers = {t for t, dt in news_first.items() if dt.timestamp() >= cutoff}
    yt_recent_tickers = {t for t, dt in yt_first.items() if dt.timestamp() >= cutoff}

    overlap = news_recent_tickers & yt_recent_tickers
    unique_news = news_recent_tickers - yt_recent_tickers

    lead_hours: list[float] = []
    for ticker in overlap:
        news_dt = news_first[ticker]
        yt_dt = yt_first[ticker]
        delta_hours = (yt_dt - news_dt).total_seconds() / 3600.0
        lead_hours.append(delta_hours)

    overlap_ratio = (
        len(overlap) / len(news_recent_tickers) * 100.0 if news_recent_tickers else 0.0
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": window_days,
        "news_tickers_total": len(news_recent_tickers),
        "news_tickers_unique": len(unique_news),
        "youtube_tickers_total": len(yt_recent_tickers),
        "overlap_ratio_pct": round(overlap_ratio, 2),
        "lead_time_summary": {
            "shared_tickers": len(overlap),
            "median_hours_news_leads_yt": (
                round(statistics.median(lead_hours), 2) if lead_hours else None
            ),
            "positive_lead_count": sum(1 for h in lead_hours if h > 0),
        },
        "news_hit_rate_5d": _hit_rate_5d(news_records),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker-db", default=".omx/state/signal_tracker.json")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--output", default="-",
                        help="Write JSON here, or '-' for stdout (default)")
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
