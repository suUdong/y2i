from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .signal_tracker import SignalRecord, SignalTrackerDB

_KR_MARKET_SUFFIXES = (".KS", ".KQ")
_EXPORTABLE_VERDICTS = {"BUY", "STRONG_BUY"}


def _is_exportable_record(record: SignalRecord) -> bool:
    ticker = str(record.ticker or "").upper()
    verdict = str(record.verdict or "").upper()
    return ticker.endswith(_KR_MARKET_SUFFIXES) and verdict in _EXPORTABLE_VERDICTS


def _record_to_kindshot_signal(record: SignalRecord) -> dict[str, Any]:
    evidence: list[str] = []
    if record.source_title:
        evidence.append(record.source_title)
    if record.price_target and record.price_target.get("target_price") is not None:
        target_price = record.price_target.get("target_price")
        currency = record.price_target.get("currency")
        target_label = f"목표가 {target_price}"
        if currency:
            target_label = f"{target_label} {currency}"
        evidence.append(target_label)
    if not evidence:
        evidence.append(f"{record.channel_slug} {record.signal_date} tracked signal")

    return {
        "ticker": record.ticker,
        "company_name": record.company_name,
        "signal_source": "y2i",
        "signal_date": record.signal_date,
        "confidence": round(max(0.0, min(1.0, float(record.signal_score or 0.0) / 100.0)), 4),
        "verdict": str(record.verdict or "").upper(),
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
