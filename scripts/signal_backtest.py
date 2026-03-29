#!/usr/bin/env python3
"""
y2i Signal Accuracy Backtest
============================
Verifies signal_tracker.json returns with pykrx (KR) / yfinance (US),
then computes precision, recall, profit contribution, and generates a report.
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
TRACKER_PATH = ROOT / "output" / "signal_tracker.json"
REPORT_PATH = ROOT / "output" / "backtest_report.json"
REPORT_MD_PATH = ROOT / "output" / "BACKTEST_REPORT.md"

# ---------------------------------------------------------------------------
# Price fetchers
# ---------------------------------------------------------------------------

def _fetch_kr_prices(ticker_code: str, start: str, end: str) -> dict[str, float]:
    """Fetch KR stock closing prices via pykrx. Returns {date_str: close}."""
    from pykrx import stock
    code = ticker_code.replace(".KS", "").replace(".KQ", "")
    try:
        df = stock.get_market_ohlcv_by_date(start.replace("-", ""), end.replace("-", ""), code)
        if df is None or df.empty:
            return {}
        return {d.strftime("%Y-%m-%d"): float(row["종가"]) for d, row in df.iterrows()}
    except Exception:
        return {}


def _fetch_us_prices(ticker: str, start: str, end: str) -> dict[str, float]:
    """Fetch US stock closing prices via yfinance. Returns {date_str: close}."""
    import yfinance as yf
    try:
        df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or df.empty:
            return {}
        return {d.strftime("%Y-%m-%d"): float(row["Close"]) for d, row in df.iterrows()}
    except Exception:
        return {}


def fetch_prices(ticker: str, start: str, end: str) -> dict[str, float]:
    if ticker.endswith((".KS", ".KQ")):
        return _fetch_kr_prices(ticker, start, end)
    return _fetch_us_prices(ticker, start, end)


# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------

def _trading_day_offset(prices_sorted: list[tuple[str, float]], entry_idx: int, days: int) -> float | None:
    """Return close price `days` trading days after entry_idx, or None."""
    target = entry_idx + days
    if 0 <= target < len(prices_sorted):
        return prices_sorted[target][1]
    return None


def verify_signal_returns(signal: dict, prices: dict[str, float]) -> dict[str, Any]:
    """Re-compute returns from raw prices for a single signal."""
    entry_price = signal.get("entry_price")
    entry_date = signal.get("entry_date")
    if not entry_price or not entry_date or not prices:
        return {"verified": False, "reason": "missing_data"}

    sorted_prices = sorted(prices.items())
    # find entry index
    entry_idx = None
    for i, (d, _) in enumerate(sorted_prices):
        if d >= entry_date:
            entry_idx = i
            break
    if entry_idx is None:
        return {"verified": False, "reason": "entry_not_in_range"}

    actual_entry = sorted_prices[entry_idx][1]
    verified_returns = {}
    for window, days in [("1d", 1), ("3d", 3), ("5d", 5), ("10d", 10), ("20d", 20)]:
        close = _trading_day_offset(sorted_prices, entry_idx, days)
        if close is not None:
            verified_returns[window] = round((close - actual_entry) / actual_entry * 100, 2)

    # compare with tracker returns
    tracker_returns = signal.get("returns", {})
    discrepancies = {}
    for w in verified_returns:
        tr = tracker_returns.get(w)
        if tr is not None and abs(verified_returns[w] - tr) > 0.5:
            discrepancies[w] = {"tracker": tr, "verified": verified_returns[w]}

    return {
        "verified": True,
        "actual_entry_price": actual_entry,
        "tracker_entry_price": entry_price,
        "entry_price_diff_pct": round((actual_entry - entry_price) / entry_price * 100, 2),
        "verified_returns": verified_returns,
        "discrepancies": discrepancies,
    }


# ---------------------------------------------------------------------------
# Metrics computation
# ---------------------------------------------------------------------------

def _safe_mean(vals: list[float]) -> float | None:
    clean = [v for v in vals if v is not None and not np.isnan(v)]
    return round(float(np.mean(clean)), 2) if clean else None


def compute_metrics(signals: list[dict]) -> dict[str, Any]:
    """Compute precision, recall, hit rates, profit contribution."""
    metrics: dict[str, Any] = {}

    # -- Overall directional accuracy (all verdicts) --
    for window in ("1d", "3d", "5d", "10d", "20d"):
        with_data = [s for s in signals if s.get("returns", {}).get(window) is not None]
        if not with_data:
            continue
        returns = [s["returns"][window] for s in with_data]
        positive = [r for r in returns if r > 0]
        metrics[f"overall_{window}"] = {
            "n": len(with_data),
            "hit_rate": round(len(positive) / len(with_data) * 100, 1),
            "avg_return": _safe_mean(returns),
            "median_return": round(float(np.median(returns)), 2),
            "win_avg": _safe_mean(positive) if positive else None,
            "loss_avg": _safe_mean([r for r in returns if r <= 0]) or None,
        }

    # -- By verdict --
    by_verdict: dict[str, dict] = {}
    for verdict in ("BUY", "WATCH", "REJECT"):
        subset = [s for s in signals if s.get("verdict") == verdict]
        if not subset:
            continue
        v_metrics: dict[str, Any] = {"count": len(subset)}
        for window in ("3d", "5d", "10d"):
            with_ret = [s for s in subset if s.get("returns", {}).get(window) is not None]
            if not with_ret:
                continue
            rets = [s["returns"][window] for s in with_ret]
            pos = [r for r in rets if r > 0]
            v_metrics[window] = {
                "n": len(with_ret),
                "hit_rate": round(len(pos) / len(with_ret) * 100, 1),
                "avg_return": _safe_mean(rets),
                "median_return": round(float(np.median(rets)), 2),
            }
        by_verdict[verdict] = v_metrics
    metrics["by_verdict"] = by_verdict

    # -- Precision / Recall for BUY --
    # Precision: of BUY signals, how many actually went up?
    # Recall: of all signals that went up (3d), how many did we mark BUY?
    buy_signals = [s for s in signals if s.get("verdict") in ("BUY", "STRONG_BUY")]
    all_with_3d = [s for s in signals if s.get("returns", {}).get("3d") is not None]
    buy_with_3d = [s for s in buy_signals if s.get("returns", {}).get("3d") is not None]

    if buy_with_3d:
        buy_correct_3d = [s for s in buy_with_3d if s["returns"]["3d"] > 0]
        metrics["precision_buy_3d"] = round(len(buy_correct_3d) / len(buy_with_3d) * 100, 1)
    if all_with_3d:
        all_positive_3d = [s for s in all_with_3d if s["returns"]["3d"] > 0]
        buy_in_positive = [s for s in all_positive_3d if s.get("verdict") in ("BUY", "STRONG_BUY")]
        metrics["recall_buy_3d"] = round(len(buy_in_positive) / len(all_positive_3d) * 100, 1) if all_positive_3d else 0

    # Same for 5d
    buy_with_5d = [s for s in buy_signals if s.get("returns", {}).get("5d") is not None]
    all_with_5d = [s for s in signals if s.get("returns", {}).get("5d") is not None]
    if buy_with_5d:
        buy_correct_5d = [s for s in buy_with_5d if s["returns"]["5d"] > 0]
        metrics["precision_buy_5d"] = round(len(buy_correct_5d) / len(buy_with_5d) * 100, 1)
    if all_with_5d:
        all_positive_5d = [s for s in all_with_5d if s["returns"]["5d"] > 0]
        buy_in_positive_5d = [s for s in all_positive_5d if s.get("verdict") in ("BUY", "STRONG_BUY")]
        metrics["recall_buy_5d"] = round(len(buy_in_positive_5d) / len(all_positive_5d) * 100, 1) if all_positive_5d else 0

    # -- By channel --
    by_channel: dict[str, dict] = {}
    channels = set(s.get("channel_slug") for s in signals)
    for ch in sorted(channels):
        subset = [s for s in signals if s.get("channel_slug") == ch]
        ch_m: dict[str, Any] = {"count": len(subset)}
        for window in ("3d", "5d", "10d"):
            with_ret = [s for s in subset if s.get("returns", {}).get(window) is not None]
            if not with_ret:
                continue
            rets = [s["returns"][window] for s in with_ret]
            pos = [r for r in rets if r > 0]
            ch_m[window] = {
                "n": len(with_ret),
                "hit_rate": round(len(pos) / len(with_ret) * 100, 1),
                "avg_return": _safe_mean(rets),
            }
        by_channel[ch] = ch_m
    metrics["by_channel"] = by_channel

    # -- Signal score correlation --
    scored_with_5d = [(s.get("signal_score", 0), s["returns"]["5d"])
                      for s in signals
                      if s.get("returns", {}).get("5d") is not None and s.get("signal_score")]
    if len(scored_with_5d) >= 10:
        from scipy.stats import spearmanr
        scores, rets = zip(*scored_with_5d)
        corr, pval = spearmanr(scores, rets)
        metrics["score_return_correlation_5d"] = {
            "spearman_r": round(corr, 3),
            "p_value": round(pval, 4),
            "n": len(scored_with_5d),
        }

    # -- Score bucket analysis --
    buckets = [(0, 50), (50, 55), (55, 60), (60, 65), (65, 70), (70, 100)]
    bucket_stats = []
    for lo, hi in buckets:
        subset = [s for s in signals
                  if (s.get("signal_score") or 0) >= lo and (s.get("signal_score") or 0) < hi
                  and s.get("returns", {}).get("5d") is not None]
        if not subset:
            continue
        rets = [s["returns"]["5d"] for s in subset]
        pos = [r for r in rets if r > 0]
        bucket_stats.append({
            "range": f"{lo}-{hi}",
            "n": len(subset),
            "hit_rate_5d": round(len(pos) / len(subset) * 100, 1),
            "avg_return_5d": _safe_mean(rets),
        })
    metrics["score_buckets"] = bucket_stats

    return metrics


def compute_kindshot_contribution(signals: list[dict]) -> dict[str, Any]:
    """Analyze signals that would flow to kindshot (KR BUY/STRONG_BUY)."""
    kr_buy = [s for s in signals
              if s.get("verdict") in ("BUY", "STRONG_BUY")
              and str(s.get("ticker", "")).endswith((".KS", ".KQ"))]

    # Also analyze WATCH signals with score >= 58 (kindshot threshold) as missed signals
    kr_watch_eligible = [s for s in signals
                         if s.get("verdict") == "WATCH"
                         and (s.get("signal_score") or 0) >= 58
                         and str(s.get("ticker", "")).endswith((".KS", ".KQ"))]

    result: dict[str, Any] = {
        "kindshot_signals_count": len(kr_buy),
        "kindshot_signals": [],
        "missed_opportunities": [],
    }

    # BUY signals → kindshot
    for s in kr_buy:
        r = s.get("returns", {})
        result["kindshot_signals"].append({
            "ticker": s["ticker"],
            "channel": s.get("channel_slug"),
            "date": s.get("signal_date"),
            "score": s.get("signal_score"),
            "entry_price": s.get("entry_price"),
            "return_3d": r.get("3d"),
            "return_5d": r.get("5d"),
            "return_10d": r.get("10d"),
            "profitable_3d": (r.get("3d") or 0) > 0,
            "profitable_5d": (r.get("5d") or 0) > 0,
        })

    # Missed opportunities: WATCH signals that actually went up significantly
    for s in kr_watch_eligible:
        r = s.get("returns", {})
        ret_5d = r.get("5d")
        if ret_5d is not None and ret_5d > 3.0:  # >3% in 5d = missed opportunity
            result["missed_opportunities"].append({
                "ticker": s["ticker"],
                "channel": s.get("channel_slug"),
                "date": s.get("signal_date"),
                "score": s.get("signal_score"),
                "return_5d": ret_5d,
                "return_10d": r.get("10d"),
            })

    # Kindshot PnL summary
    if kr_buy:
        rets_3d = [s["returns"].get("3d") for s in kr_buy if s.get("returns", {}).get("3d") is not None]
        rets_5d = [s["returns"].get("5d") for s in kr_buy if s.get("returns", {}).get("5d") is not None]
        result["pnl_summary"] = {
            "avg_return_3d": _safe_mean(rets_3d),
            "avg_return_5d": _safe_mean(rets_5d),
            "win_rate_3d": round(len([r for r in rets_3d if r > 0]) / len(rets_3d) * 100, 1) if rets_3d else None,
            "win_rate_5d": round(len([r for r in rets_5d if r > 0]) / len(rets_5d) * 100, 1) if rets_5d else None,
            "total_signals": len(kr_buy),
            "cumulative_return_3d": round(sum(rets_3d), 2) if rets_3d else None,
            "cumulative_return_5d": round(sum(rets_5d), 2) if rets_5d else None,
        }

    return result


# ---------------------------------------------------------------------------
# Price verification (sample)
# ---------------------------------------------------------------------------

def verify_sample_prices(signals: list[dict], sample_size: int = 15) -> list[dict]:
    """Verify a sample of signal prices against pykrx/yfinance."""
    import random
    # prioritize BUY signals + random sample
    buy = [s for s in signals if s.get("verdict") in ("BUY", "STRONG_BUY") and s.get("entry_price")]
    others = [s for s in signals if s.get("verdict") not in ("BUY", "STRONG_BUY") and s.get("entry_price")]
    sample = buy + random.sample(others, min(sample_size - len(buy), len(others)))

    results = []
    seen_tickers = set()
    for s in sample:
        ticker = s["ticker"]
        if ticker in seen_tickers:
            continue
        seen_tickers.add(ticker)

        entry_date = s.get("entry_date") or s.get("signal_date")
        if not entry_date:
            continue

        start_dt = datetime.strptime(entry_date, "%Y-%m-%d") - timedelta(days=1)
        end_dt = start_dt + timedelta(days=35)
        prices = fetch_prices(ticker, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))

        vr = verify_signal_returns(s, prices)
        vr["ticker"] = ticker
        vr["verdict"] = s.get("verdict")
        vr["signal_date"] = s.get("signal_date")
        results.append(vr)
        disc_info = vr.get("discrepancies")
        status = "OK" if not disc_info else f"DISCREPANCY {disc_info}"
        print(f"  Verified {ticker}: {status}")

    return results


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_markdown_report(
    metrics: dict,
    kindshot: dict,
    verification: list[dict],
    total_signals: int,
) -> str:
    lines = [
        "# Y2I Signal Accuracy Backtest Report",
        f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**Total Signals**: {total_signals}",
        "",
        "---",
        "",
        "## 1. Overall Directional Accuracy",
        "",
        "| Window | N | Hit Rate | Avg Return | Median Return | Win Avg | Loss Avg |",
        "|--------|---|----------|------------|---------------|---------|----------|",
    ]

    for window in ("1d", "3d", "5d", "10d", "20d"):
        m = metrics.get(f"overall_{window}")
        if not m:
            continue
        lines.append(
            f"| {window} | {m['n']} | {m['hit_rate']}% | {m['avg_return']}% | {m['median_return']}% | {m.get('win_avg', 'N/A')}% | {m.get('loss_avg', 'N/A')}% |"
        )

    # Precision / Recall
    lines += [
        "",
        "## 2. BUY Signal Precision & Recall",
        "",
    ]
    p3 = metrics.get("precision_buy_3d", "N/A")
    r3 = metrics.get("recall_buy_3d", "N/A")
    p5 = metrics.get("precision_buy_5d", "N/A")
    r5 = metrics.get("recall_buy_5d", "N/A")
    lines += [
        f"| Metric | 3-Day | 5-Day |",
        f"|--------|-------|-------|",
        f"| Precision (BUY correct %) | {p3}% | {p5}% |",
        f"| Recall (captured winners %) | {r3}% | {r5}% |",
        "",
        "> **Precision**: When we say BUY, how often does the stock actually go up?",
        "> **Recall**: Of all stocks that went up, how many did we flag as BUY?",
    ]

    # By verdict
    lines += ["", "## 3. Performance by Verdict", ""]
    bv = metrics.get("by_verdict", {})
    lines += ["| Verdict | Count | 3d Hit Rate | 3d Avg Return | 5d Hit Rate | 5d Avg Return |",
              "|---------|-------|-------------|---------------|-------------|---------------|"]
    for v in ("BUY", "WATCH", "REJECT"):
        vm = bv.get(v, {})
        d3 = vm.get("3d", {})
        d5 = vm.get("5d", {})
        lines.append(
            f"| {v} | {vm.get('count', 0)} | {d3.get('hit_rate', 'N/A')}% | {d3.get('avg_return', 'N/A')}% | {d5.get('hit_rate', 'N/A')}% | {d5.get('avg_return', 'N/A')}% |"
        )

    # Score correlation
    corr = metrics.get("score_return_correlation_5d")
    if corr:
        lines += [
            "",
            "## 4. Signal Score vs Return Correlation",
            "",
            f"- **Spearman r**: {corr['spearman_r']} (p={corr['p_value']}, n={corr['n']})",
            f"- Interpretation: {'Significant' if corr['p_value'] < 0.05 else 'Not significant'} — "
            f"{'higher scores predict better returns' if corr['spearman_r'] > 0 else 'no clear predictive power'}",
        ]

    # Score buckets
    buckets = metrics.get("score_buckets", [])
    if buckets:
        lines += ["", "## 5. Performance by Score Bucket", "",
                   "| Score Range | N | Hit Rate (5d) | Avg Return (5d) |",
                   "|-------------|---|---------------|-----------------|"]
        for b in buckets:
            lines.append(f"| {b['range']} | {b['n']} | {b['hit_rate_5d']}% | {b['avg_return_5d']}% |")

    # By channel
    by_ch = metrics.get("by_channel", {})
    if by_ch:
        lines += ["", "## 6. Channel Accuracy Ranking", "",
                   "| Channel | Count | 3d Hit | 3d Avg | 5d Hit | 5d Avg |",
                   "|---------|-------|--------|--------|--------|--------|"]
        ch_sorted = sorted(by_ch.items(),
                           key=lambda x: x[1].get("5d", {}).get("avg_return") or -999, reverse=True)
        for ch, m in ch_sorted:
            d3 = m.get("3d", {})
            d5 = m.get("5d", {})
            lines.append(
                f"| {ch} | {m['count']} | {d3.get('hit_rate', 'N/A')}% | {d3.get('avg_return', 'N/A')}% | {d5.get('hit_rate', 'N/A')}% | {d5.get('avg_return', 'N/A')}% |"
            )

    # Kindshot contribution
    lines += ["", "## 7. Kindshot Contribution Analysis", ""]
    pnl = kindshot.get("pnl_summary", {})
    if pnl:
        lines += [
            f"- **Signals sent to kindshot**: {pnl.get('total_signals', 0)}",
            f"- **3d Win Rate**: {pnl.get('win_rate_3d', 'N/A')}%  |  **Avg Return**: {pnl.get('avg_return_3d', 'N/A')}%",
            f"- **5d Win Rate**: {pnl.get('win_rate_5d', 'N/A')}%  |  **Avg Return**: {pnl.get('avg_return_5d', 'N/A')}%",
            f"- **Cumulative Return (3d)**: {pnl.get('cumulative_return_3d', 'N/A')}%",
            f"- **Cumulative Return (5d)**: {pnl.get('cumulative_return_5d', 'N/A')}%",
            "",
        ]

    ks = kindshot.get("kindshot_signals", [])
    if ks:
        lines += ["### Kindshot Signal Detail", "",
                   "| Ticker | Channel | Date | Score | 3d | 5d | 10d |",
                   "|--------|---------|------|-------|-----|-----|------|"]
        for s in ks:
            lines.append(
                f"| {s['ticker']} | {s['channel']} | {s['date']} | {s['score']} | {s.get('return_3d', 'N/A')}% | {s.get('return_5d', 'N/A')}% | {s.get('return_10d', 'N/A')}% |"
            )

    missed = kindshot.get("missed_opportunities", [])
    if missed:
        lines += ["", "### Missed Opportunities (WATCH but >3% in 5d)", "",
                   "| Ticker | Channel | Date | Score | 5d Return | 10d Return |",
                   "|--------|---------|------|-------|-----------|------------|"]
        for s in sorted(missed, key=lambda x: -(x.get("return_5d") or 0)):
            lines.append(
                f"| {s['ticker']} | {s['channel']} | {s['date']} | {s['score']} | +{s['return_5d']}% | {s.get('return_10d', 'N/A')}% |"
            )

    # Price verification
    if verification:
        disc = [v for v in verification if v.get("discrepancies")]
        lines += [
            "", "## 8. Price Verification (pykrx/yfinance)", "",
            f"- **Verified**: {len(verification)} signals",
            f"- **Discrepancies found**: {len(disc)}",
        ]
        if disc:
            lines += ["", "| Ticker | Window | Tracker | Verified | Delta |",
                       "|--------|--------|---------|----------|-------|"]
            for v in disc:
                for w, d in v["discrepancies"].items():
                    lines.append(f"| {v['ticker']} | {w} | {d['tracker']}% | {d['verified']}% | {round(d['verified'] - d['tracker'], 2)}% |")

    # Key findings
    lines += [
        "", "---", "",
        "## Key Findings & Recommendations",
        "",
    ]

    # Auto-generate findings
    findings = []

    # BUY precision
    if isinstance(p3, (int, float)) and p3 >= 60:
        findings.append(f"BUY signal precision is strong at {p3}% (3d) — signals reaching BUY threshold are reliable")
    elif isinstance(p3, (int, float)):
        findings.append(f"BUY signal precision is {p3}% (3d) — needs improvement before heavy kindshot reliance")

    # Recall
    if isinstance(r3, (int, float)) and r3 < 10:
        findings.append(f"Recall is very low ({r3}%) — most winning stocks are NOT being flagged as BUY. Consider lowering BUY threshold or reviewing WATCH→BUY promotion logic")

    # Best channel
    if by_ch:
        best_ch = max(by_ch.items(), key=lambda x: x[1].get("5d", {}).get("avg_return") or -999)
        worst_ch = min(by_ch.items(), key=lambda x: x[1].get("5d", {}).get("avg_return") or 999)
        findings.append(f"Best channel: **{best_ch[0]}** (5d avg {best_ch[1].get('5d', {}).get('avg_return')}%)")
        findings.append(f"Worst channel: **{worst_ch[0]}** (5d avg {worst_ch[1].get('5d', {}).get('avg_return')}%)")

    # Score correlation
    if corr and corr["spearman_r"] > 0.1 and corr["p_value"] < 0.05:
        findings.append(f"Signal score IS predictive of returns (r={corr['spearman_r']}, p={corr['p_value']})")
    elif corr:
        findings.append(f"Signal score shows weak/no correlation with actual returns (r={corr['spearman_r']})")

    # Missed opportunities
    if missed:
        findings.append(f"**{len(missed)} missed opportunities** — WATCH signals that gained >3% in 5 days. Review signal gate thresholds")

    for i, f in enumerate(findings, 1):
        lines.append(f"{i}. {f}")

    lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Loading signal tracker...")
    with open(TRACKER_PATH) as f:
        data = json.load(f)
    signals = data["signals"]
    print(f"  {len(signals)} signals loaded")

    # Step 1: Compute metrics
    print("\nComputing metrics...")
    metrics = compute_metrics(signals)

    # Step 2: Kindshot contribution
    print("Analyzing kindshot contribution...")
    kindshot = compute_kindshot_contribution(signals)

    # Step 3: Verify sample prices
    print("\nVerifying sample prices with pykrx/yfinance...")
    verification = verify_sample_prices(signals, sample_size=15)

    # Step 4: Generate reports
    print("\nGenerating reports...")
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_signals": len(signals),
        "metrics": metrics,
        "kindshot_contribution": kindshot,
        "verification": verification,
    }

    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    print(f"  JSON report: {REPORT_PATH}")

    md = generate_markdown_report(metrics, kindshot, verification, len(signals))
    with open(REPORT_MD_PATH, "w") as f:
        f.write(md)
    print(f"  Markdown report: {REPORT_MD_PATH}")

    # Print summary
    print("\n" + "=" * 60)
    print("BACKTEST SUMMARY")
    print("=" * 60)
    for window in ("3d", "5d"):
        m = metrics.get(f"overall_{window}")
        if m:
            print(f"  {window}: hit_rate={m['hit_rate']}% avg={m['avg_return']}% (n={m['n']})")
    p3 = metrics.get("precision_buy_3d", "N/A")
    r3 = metrics.get("recall_buy_3d", "N/A")
    print(f"  BUY precision(3d): {p3}%  recall(3d): {r3}%")
    pnl = kindshot.get("pnl_summary", {})
    if pnl:
        print(f"  Kindshot: {pnl.get('total_signals')} signals, 5d win_rate={pnl.get('win_rate_5d')}%, avg_ret={pnl.get('avg_return_5d')}%")
    print(f"  Missed opportunities (WATCH >3% 5d): {len(kindshot.get('missed_opportunities', []))}")


if __name__ == "__main__":
    main()
