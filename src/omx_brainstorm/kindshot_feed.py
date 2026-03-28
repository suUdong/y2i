from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .signal_tracker import build_recent_consensus_signals
from .signal_tracker import SignalRecord, SignalTrackerDB

_KR_MARKET_SUFFIXES = (".KS", ".KQ")
_EXPORTABLE_VERDICTS = {"BUY", "STRONG_BUY"}
_MIN_KINDSHOT_SIGNAL_SCORE = 58.0
_MIN_KINDSHOT_HIGH_CONFIDENCE_SCORE = 72.0
_KINDSHOT_DIRECTIONAL_WINDOWS = ("3d", "5d")
_MIN_KINDSHOT_CHANNEL_WEIGHT = 0.9
_TICKER_CHANNEL_COOLDOWN_DAYS = 7


def _is_exportable_record(
    record: SignalRecord,
    *,
    channel_weights: dict[str, float] | None = None,
    consensus_by_ticker: dict[str, dict[str, Any]] | None = None,
) -> bool:
    ticker = str(record.ticker or "").upper()
    verdict = str(record.verdict or "").upper()
    score = float(record.signal_score or 0.0)
    has_target = isinstance(record.price_target, dict) and record.price_target.get("target_price") is not None
    has_strong_conviction = verdict == "STRONG_BUY" or score >= _MIN_KINDSHOT_HIGH_CONFIDENCE_SCORE
    channel_weight = max(0.1, float((channel_weights or {}).get(record.channel_slug, 1.0) or 1.0))
    consensus = (consensus_by_ticker or {}).get(ticker, {})
    has_consensus_support = bool(consensus.get("consensus_signal"))
    has_positive_history = _has_positive_directional_history(record)
    return (
        ticker.endswith(_KR_MARKET_SUFFIXES)
        and verdict in _EXPORTABLE_VERDICTS
        and score >= _MIN_KINDSHOT_SIGNAL_SCORE
        and (has_target or has_strong_conviction)
        and (has_strong_conviction or has_positive_history or has_consensus_support)
        and (has_consensus_support or channel_weight >= _MIN_KINDSHOT_CHANNEL_WEIGHT)
        and not _has_failed_directional_history(record)
    )


def _record_to_kindshot_signal(
    record: SignalRecord,
    *,
    channel_weight: float = 1.0,
    consensus: dict[str, Any] | None = None,
) -> dict[str, Any]:
    verdict = str(record.verdict or "").upper()
    evidence: list[str] = [f"점수 {float(record.signal_score or 0.0):.1f} | {verdict} | {record.channel_slug}"]
    if channel_weight != 1.0:
        evidence.append(f"채널 가중치 {channel_weight:.2f}x")
    if consensus and consensus.get("channel_count", 0) >= 2:
        consensus_label = "합의 통과" if consensus.get("consensus_signal") else "합의 후보"
        evidence.append(
            f"{consensus_label} | {consensus.get('consensus_strength')} | "
            f"{int(consensus.get('channel_count', 0) or 0)}채널 | xval {float(consensus.get('cross_validation_score', 0) or 0):.1f}"
        )
    if record.source_title:
        evidence.append(record.source_title)
    if record.price_target and record.price_target.get("target_price") is not None:
        target_price = record.price_target.get("target_price")
        currency = record.price_target.get("currency")
        target_label = f"목표가 {target_price}"
        if currency:
            target_label = f"{target_label} {currency}"
        evidence.append(target_label)
    if record.target_progress_pct is not None:
        evidence.append(f"목표 진척 {float(record.target_progress_pct):.1f}%")
    for window_key in _KINDSHOT_DIRECTIONAL_WINDOWS:
        directional_return = _directional_return(record, window_key)
        if directional_return is not None:
            evidence.append(f"{window_key} 방향수익률 {float(directional_return):.2f}%")
    if not evidence:
        evidence.append(f"{record.channel_slug} {record.signal_date} tracked signal")

    confidence = float(record.signal_score or 0.0) / 100.0
    if verdict == "STRONG_BUY":
        confidence += 0.05
    if record.price_target and record.price_target.get("target_price") is not None:
        confidence += 0.03
    confidence += max(-0.04, min(0.05, (float(channel_weight or 1.0) - 1.0) * 0.18))
    if consensus and consensus.get("consensus_signal"):
        confidence += 0.06
    elif consensus and consensus.get("channel_count", 0) >= 2:
        confidence -= 0.02
    directional_5d = _directional_return(record, "5d")
    if directional_5d is not None:
        confidence += max(-0.04, min(0.05, directional_5d / 100.0))

    return {
        "ticker": record.ticker,
        "company_name": record.company_name,
        "signal_source": "y2i",
        "signal_date": record.signal_date,
        "confidence": round(max(0.0, min(0.99, confidence)), 4),
        "verdict": verdict,
        "channel": record.channel_slug,
        "channel_weight": round(float(channel_weight or 1.0), 3),
        "consensus_signal": bool(consensus and consensus.get("consensus_signal")),
        "consensus_strength": (consensus or {}).get("consensus_strength"),
        "consensus_channel_count": int((consensus or {}).get("channel_count", 0) or 0),
        "evidence": evidence,
    }


def _has_failed_directional_history(record: SignalRecord) -> bool:
    directional_5d = _directional_return(record, "5d")
    if directional_5d is not None:
        return directional_5d < 0.5

    short_values = [
        float(value)
        for value in (_directional_return(record, "1d"), _directional_return(record, "3d"))
        if value is not None
    ]
    return len(short_values) >= 2 and (sum(short_values) / len(short_values)) < 0.5


def _has_positive_directional_history(record: SignalRecord) -> bool:
    directional_5d = _directional_return(record, "5d")
    if directional_5d is not None:
        return directional_5d > 0

    short_values = [
        float(value)
        for value in (_directional_return(record, "1d"), _directional_return(record, "3d"))
        if value is not None
    ]
    return bool(short_values) and (sum(short_values) / len(short_values)) > 0


def _directional_return(record: SignalRecord, window_key: str) -> float | None:
    raw_return = record.returns.get(window_key)
    if raw_return is None:
        return None
    verdict = str(record.verdict or "").upper()
    if verdict in {"SELL", "REJECT", "AVOID"}:
        return round(-float(raw_return), 2)
    return round(float(raw_return), 2)


def _build_consensus_by_ticker(
    db: SignalTrackerDB,
    *,
    channel_weights: dict[str, float] | None = None,
) -> dict[str, dict[str, Any]]:
    by_ticker: dict[str, dict[str, Any]] = {}
    for item in build_recent_consensus_signals(
        db.records,
        channel_weights=channel_weights or {},
        limit=None,
        qualified_only=False,
    ):
        ticker = str(item.get("ticker", "")).upper()
        if not ticker:
            continue
        current = by_ticker.get(ticker)
        candidate_key = (
            bool(item.get("consensus_signal")),
            float(item.get("aggregate_score", 0) or 0),
            float(item.get("cross_validation_score", 0) or 0),
            int(item.get("channel_count", 0) or 0),
        )
        current_key = (
            bool((current or {}).get("consensus_signal")),
            float((current or {}).get("aggregate_score", 0) or 0),
            float((current or {}).get("cross_validation_score", 0) or 0),
            int((current or {}).get("channel_count", 0) or 0),
        )
        if current is None or candidate_key > current_key:
            by_ticker[ticker] = item
    return by_ticker


def _dedup_signals(records: list[SignalRecord]) -> list[SignalRecord]:
    """Keep only the highest-scoring signal per ticker+channel within the cooldown window."""
    best: dict[tuple[str, str], SignalRecord] = {}
    for record in records:
        key = (str(record.ticker or "").upper(), record.channel_slug)
        existing = best.get(key)
        if existing is None:
            best[key] = record
            continue
        try:
            existing_dt = datetime.fromisoformat(existing.signal_date[:10])
            record_dt = datetime.fromisoformat(record.signal_date[:10])
        except (ValueError, TypeError):
            best[key] = record
            continue
        if abs((existing_dt - record_dt).days) <= _TICKER_CHANNEL_COOLDOWN_DAYS:
            if float(record.signal_score or 0) > float(existing.signal_score or 0):
                best[key] = record
        else:
            best[key] = record
    return list(best.values())


def export_signals_for_kindshot(
    db: SignalTrackerDB,
    output_path: Path,
    *,
    channel_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    consensus_by_ticker = _build_consensus_by_ticker(db, channel_weights=channel_weights)
    exportable = sorted(
        (
            item for item in db.records
            if _is_exportable_record(
                item,
                channel_weights=channel_weights,
                consensus_by_ticker=consensus_by_ticker,
            )
        ),
        key=lambda item: (item.signal_date, item.signal_score, item.channel_slug, item.ticker),
        reverse=True,
    )
    deduped = _dedup_signals(exportable)
    signals = [
        _record_to_kindshot_signal(
            record,
            channel_weight=float((channel_weights or {}).get(record.channel_slug, 1.0) or 1.0),
            consensus=consensus_by_ticker.get(str(record.ticker or "").upper()),
        )
        for record in sorted(
            deduped,
            key=lambda item: (item.signal_date, item.signal_score, item.channel_slug, item.ticker),
            reverse=True,
        )
    ]
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "signals": signals,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "path": str(output_path),
        "signal_count": len(signals),
        "generated_at": payload["generated_at"],
    }
