"""Backtest the kindshot signal exporter against tracked signal returns.

Loads ``.omx/state/signal_tracker.json``, runs ``export_signals_for_kindshot`` on
the in-memory DB, then evaluates the realised 5d directional returns of the
exported tickers using the tracker's history.

Usage::

    python -m scripts.backtest_kindshot_export
    python -m scripts.backtest_kindshot_export --window 5d --min-score 65
"""
from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from omx_brainstorm.signal_tracker import SignalRecord, SignalTrackerDB  # noqa: E402
from omx_brainstorm.kindshot_feed import export_signals_for_kindshot  # noqa: E402

DEFAULT_TRACKER = ROOT / ".omx" / "state" / "signal_tracker.json"


def _safe_float(value) -> float | None:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f):
        return None
    return f


def _signed_direction(verdict: str | None) -> int:
    v = str(verdict or "").strip().upper()
    return -1 if v in {"SELL", "STRONG_SELL", "AVOID", "REJECT", "SHORT"} else 1


def _directional_return(record: SignalRecord, window: str) -> float | None:
    raw = _safe_float(record.returns.get(window))
    if raw is None:
        return None
    return raw * _signed_direction(record.verdict)


def _record_index(records: Iterable[SignalRecord]) -> dict[tuple[str, str, str], SignalRecord]:
    by_key: dict[tuple[str, str, str], SignalRecord] = {}
    for r in records:
        by_key[(r.ticker, r.channel_slug, r.signal_date[:10])] = r
    return by_key


def _strip_returns(db: SignalTrackerDB) -> SignalTrackerDB:
    """Return a copy of ``db`` with realised returns wiped so the exporter cannot
    use look-ahead 1d/3d/5d returns when selecting fresh signals."""
    from dataclasses import replace

    tmp_path = Path("/tmp/kindshot_feed_backtest_db.json")
    cloned = SignalTrackerDB(tmp_path)
    cloned._records = []
    for rec in db.records:
        wiped = replace(rec, returns={k: None for k in rec.returns}, latest_price=None, latest_price_date=None,
                        price_path=[], target_progress_pct=None, target_distance_pct=None,
                        target_hit=False, target_hit_date=None)
        cloned._records.append(wiped)
    return cloned


def evaluate(
    db: SignalTrackerDB,
    *,
    window: str = "5d",
    channel_weights: dict[str, float] | None = None,
    fresh_signal_mode: bool = False,
) -> dict:
    """Run the exporter, then evaluate realised directional returns.

    fresh_signal_mode=True strips realised returns before the exporter runs so the
    filter cannot use look-ahead information. The realised returns are then read
    back from the original db to compute win rate / avg directional return.
    """
    tmp_path = Path("/tmp/kindshot_feed_backtest.json")
    exporter_db = _strip_returns(db) if fresh_signal_mode else db
    payload = export_signals_for_kindshot(exporter_db, tmp_path, channel_weights=channel_weights or {})
    exported = json.loads(tmp_path.read_text())
    by_key = _record_index(db.records)

    matched_returns: list[float] = []
    nan_skipped = 0
    missing = 0
    for sig in exported["signals"]:
        ticker = sig.get("ticker", "")
        channel = sig.get("channel") or sig.get("channel_slug", "")
        signal_date = (sig.get("signal_date") or "")[:10]
        rec = by_key.get((ticker, channel, signal_date))
        if rec is None:
            missing += 1
            continue
        d = _directional_return(rec, window)
        if d is None:
            nan_skipped += 1
            continue
        matched_returns.append(d)

    if matched_returns:
        wins = sum(1 for v in matched_returns if v > 0)
        win_rate = wins / len(matched_returns) * 100
        avg = statistics.mean(matched_returns)
        median = statistics.median(matched_returns)
        best = max(matched_returns)
        worst = min(matched_returns)
    else:
        win_rate = avg = median = best = worst = None

    return {
        "exported_count": payload["signal_count"],
        "matured_count": len(matched_returns),
        "nan_or_pending": nan_skipped,
        "unmatched": missing,
        "win_rate_pct": round(win_rate, 2) if win_rate is not None else None,
        "avg_directional_return_pct": round(avg, 2) if avg is not None else None,
        "median_directional_return_pct": round(median, 2) if median is not None else None,
        "best_pct": round(best, 2) if best is not None else None,
        "worst_pct": round(worst, 2) if worst is not None else None,
    }


def _channel_weights_from_records(db: SignalTrackerDB) -> dict[str, float]:
    """Approximate channel weights from cross-channel win rates (5d, KR BUY only)."""
    by_ch: dict[str, list[float]] = {}
    for r in db.records:
        if not r.ticker.endswith((".KS", ".KQ")):
            continue
        if str(r.verdict or "").upper() not in {"BUY", "STRONG_BUY"}:
            continue
        d = _directional_return(r, "5d")
        if d is None:
            continue
        by_ch.setdefault(r.channel_slug, []).append(d)
    weights: dict[str, float] = {}
    for ch, rets in by_ch.items():
        if len(rets) < 5:
            weights[ch] = 1.0
            continue
        win_rate = sum(1 for v in rets if v > 0) / len(rets)
        weights[ch] = max(0.4, min(1.5, 0.6 + win_rate))
    return weights


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tracker", type=Path, default=DEFAULT_TRACKER)
    parser.add_argument("--window", default="5d")
    parser.add_argument("--use-empirical-weights", action="store_true",
                        help="Compute channel weights from per-channel win rates")
    parser.add_argument("--fresh", action="store_true",
                        help="Strip realised returns before exporting (simulates live filter on fresh signals)")
    args = parser.parse_args()

    if not args.tracker.exists():
        print(f"tracker not found: {args.tracker}", file=sys.stderr)
        return 2

    db = SignalTrackerDB(args.tracker)
    print(f"loaded {len(db.records)} records from {args.tracker}")

    weights = _channel_weights_from_records(db) if args.use_empirical_weights else None
    if weights:
        print("empirical channel weights (BUY-only 5d win rate):")
        for ch, w in sorted(weights.items(), key=lambda x: -x[1]):
            print(f"  {ch:<14} {w:.3f}")

    result = evaluate(db, window=args.window, channel_weights=weights, fresh_signal_mode=args.fresh)
    print()
    print(f"mode:              {'fresh-signal (no look-ahead)' if args.fresh else 'as-is (look-ahead allowed)'}")
    print(f"window:            {args.window}")
    print(f"exported:          {result['exported_count']}")
    print(f"matured (returns): {result['matured_count']}")
    print(f"nan or pending:    {result['nan_or_pending']}")
    print(f"win rate:          {result['win_rate_pct']}%")
    print(f"avg directional:   {result['avg_directional_return_pct']}%")
    print(f"median:            {result['median_directional_return_pct']}%")
    print(f"best/worst:        {result['best_pct']}% / {result['worst_pct']}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
