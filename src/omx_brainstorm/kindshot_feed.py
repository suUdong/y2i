from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .signal_tracker import SignalRecord, SignalTrackerDB

_KR_MARKET_SUFFIXES = (".KS", ".KQ")
_EXPORTABLE_VERDICTS = {"BUY", "STRONG_BUY"}
_MIN_KINDSHOT_SIGNAL_SCORE = 65.0
_MIN_KINDSHOT_HIGH_CONFIDENCE_SCORE = 80.0


def _is_exportable_record(record: SignalRecord) -> bool:
    ticker = str(record.ticker or "").upper()
    verdict = str(record.verdict or "").upper()
    score = float(record.signal_score or 0.0)
    has_target = isinstance(record.price_target, dict) and record.price_target.get("target_price") is not None
    has_strong_conviction = verdict == "STRONG_BUY" or score >= _MIN_KINDSHOT_HIGH_CONFIDENCE_SCORE
    return (
        ticker.endswith(_KR_MARKET_SUFFIXES)
        and verdict in _EXPORTABLE_VERDICTS
        and score >= _MIN_KINDSHOT_SIGNAL_SCORE
        and (has_target or has_strong_conviction)
    )


def _record_to_kindshot_signal(record: SignalRecord) -> dict[str, Any]:
    verdict = str(record.verdict or "").upper()
    evidence: list[str] = [f"점수 {float(record.signal_score or 0.0):.1f} | {verdict} | {record.channel_slug}"]
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
    if not evidence:
        evidence.append(f"{record.channel_slug} {record.signal_date} tracked signal")

    confidence = float(record.signal_score or 0.0) / 100.0
    if verdict == "STRONG_BUY":
        confidence += 0.05
    if record.price_target and record.price_target.get("target_price") is not None:
        confidence += 0.03

    return {
        "ticker": record.ticker,
        "company_name": record.company_name,
        "signal_source": "y2i",
        "signal_date": record.signal_date,
        "confidence": round(max(0.0, min(0.99, confidence)), 4),
        "verdict": verdict,
        "channel": record.channel_slug,
        "evidence": evidence,
    }


def export_signals_for_kindshot(db: SignalTrackerDB, output_path: Path) -> dict[str, Any]:
    signals = [
        _record_to_kindshot_signal(record)
        for record in sorted(
            (item for item in db.records if _is_exportable_record(item)),
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
