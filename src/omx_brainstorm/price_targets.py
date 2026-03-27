from __future__ import annotations

import re
from statistics import mean
from typing import Any, Iterable

from .models import PriceTarget
from .stock_registry import COMPANY_MAP
from .utils import normalize_ws, split_sentences, unique_preserve

STRONG_TARGET_CUES = (
    "목표가",
    "price target",
    "target price",
    "타겟",
    "target",
    "도달",
    "레벨",
    "까지",
    "간다",
    "간다고",
    "볼 수",
    "본다",
)
BEARISH_CUES = ("하락", "내려", "조정", "하단", "downside", "bear", "리스크")
CURRENT_PRICE_CUES = ("현재가", "지금", "종가", "시초가", "오늘 종가", "현재")
HORIZON_RE = re.compile(r"(올해|연말|내년|이번 분기|다음 분기|\d+\s*(?:개월|주|일)|\d+\s*달\s*(?:후|안|내))")
USD_PREFIX_RE = re.compile(r"\$\s*(\d{1,5}(?:[.,]\d{1,2})?)")
USD_SUFFIX_RE = re.compile(r"(\d{1,5}(?:[.,]\d{1,2})?)\s*(?:달러|불|usd)", re.I)
KRW_MAN_RE = re.compile(r"(\d{1,4}(?:[.,]\d+)?)\s*만\s*원?")
KRW_WON_RE = re.compile(r"(\d{1,3}(?:,\d{3})+)\s*원")


def extract_price_targets(
    text: str,
    *,
    ticker: str,
    company_name: str | None = None,
    current_price: float | None = None,
    currency: str | None = None,
    max_targets: int = 1,
) -> list[PriceTarget]:
    aliases = _ticker_aliases(ticker, company_name)
    if not aliases:
        return []

    candidates: list[tuple[float, PriceTarget]] = []
    for sentence in split_sentences(text):
        normalized = normalize_ws(sentence)
        lower = normalized.lower()
        if not any(alias in lower for alias in aliases):
            continue
        if not any(cue in lower for cue in STRONG_TARGET_CUES):
            continue

        for price, explicit_currency, span in _extract_price_values(normalized):
            inferred_currency = explicit_currency or currency
            if not _looks_like_target_price(
                price,
                current_price=current_price,
                currency=inferred_currency,
                explicit_currency=explicit_currency is not None,
            ):
                continue
            score = _candidate_score(normalized, lower, span, price, current_price)
            direction = _infer_direction(price, current_price, lower)
            horizon_match = HORIZON_RE.search(normalized)
            reasoning = "가격 타겟 문맥에서 추출"
            evidence = [normalized[:220]]
            target = PriceTarget(
                target_price=round(price, 4),
                currency=inferred_currency,
                confidence=round(min(0.95, 0.45 + score * 0.1), 2),
                direction=direction,
                time_horizon=horizon_match.group(1) if horizon_match else None,
                reasoning=reasoning,
                evidence=evidence,
            )
            candidates.append((score, target))

    if not candidates:
        return []
    candidates.sort(key=lambda item: (-item[0], -item[1].confidence, -item[1].target_price))

    selected: list[PriceTarget] = []
    seen: set[tuple[float, str | None]] = set()
    for _score, target in candidates:
        key = (target.target_price, target.currency)
        if key in seen:
            continue
        seen.add(key)
        selected.append(target)
        if len(selected) >= max_targets:
            break
    return selected


def aggregate_price_targets(
    targets: Iterable[PriceTarget | dict[str, Any]],
    *,
    latest_price: float | None = None,
    currency: str | None = None,
) -> dict[str, Any] | None:
    normalized_targets = [_coerce_target_dict(item) for item in targets]
    normalized_targets = [item for item in normalized_targets if item.get("target_price") is not None]
    if not normalized_targets:
        return None

    prices = [float(item["target_price"]) for item in normalized_targets]
    target_price = round(mean(prices), 4)
    resolved_currency = currency or next((item.get("currency") for item in normalized_targets if item.get("currency")), None)
    direction = next((str(item.get("direction")) for item in normalized_targets if item.get("direction")), "UP")
    if latest_price is not None:
        direction = "UP" if target_price >= float(latest_price) else "DOWN"

    status = "PENDING"
    current_vs_target_pct = None
    if latest_price is not None and latest_price > 0:
        current_vs_target_pct = round((target_price - float(latest_price)) / float(latest_price) * 100, 2)
        if (direction == "UP" and float(latest_price) >= target_price) or (direction == "DOWN" and float(latest_price) <= target_price):
            status = "HIT"

    return {
        "target_price": target_price,
        "target_low": round(min(prices), 4),
        "target_high": round(max(prices), 4),
        "target_count": len(normalized_targets),
        "currency": resolved_currency,
        "confidence": round(mean(float(item.get("confidence", 0) or 0) for item in normalized_targets), 2),
        "direction": direction,
        "time_horizon": next((item.get("time_horizon") for item in normalized_targets if item.get("time_horizon")), None),
        "evidence": unique_preserve(
            evidence
            for item in normalized_targets
            for evidence in list(item.get("evidence", []) or [])
            if evidence
        )[:3],
        "current_price": latest_price,
        "current_vs_target_pct": current_vs_target_pct,
        "status": status,
    }


def _ticker_aliases(ticker: str, company_name: str | None) -> list[str]:
    aliases: list[str] = []
    for key, (mapped_ticker, _mapped_company) in COMPANY_MAP.items():
        if mapped_ticker.upper() == ticker.upper():
            aliases.append(key.lower())
    stripped = ticker.lower().split(".", 1)[0]
    aliases.extend([ticker.lower(), stripped])
    if company_name:
        aliases.append(company_name.lower())
    return [alias for alias in unique_preserve(aliases) if alias]


def _extract_price_values(text: str) -> list[tuple[float, str | None, tuple[int, int]]]:
    results: list[tuple[float, str | None, tuple[int, int]]] = []
    for pattern, resolved_currency, multiplier in (
        (USD_PREFIX_RE, "USD", 1.0),
        (USD_SUFFIX_RE, "USD", 1.0),
        (KRW_MAN_RE, "KRW", 10_000.0),
        (KRW_WON_RE, "KRW", 1.0),
    ):
        for match in pattern.finditer(text):
            raw = match.group(1).replace(",", "")
            try:
                price = float(raw) * multiplier
            except ValueError:
                continue
            results.append((price, resolved_currency, match.span()))
    return results


def _looks_like_target_price(
    price: float,
    *,
    current_price: float | None,
    currency: str | None,
    explicit_currency: bool,
) -> bool:
    if price <= 0:
        return False
    if current_price is None or current_price <= 0:
        return True

    ratio = price / float(current_price)
    if explicit_currency:
        return 0.15 <= ratio <= 12.0
    if currency == "KRW":
        return 0.4 <= ratio <= 4.0
    return 0.4 <= ratio <= 6.0


def _candidate_score(
    sentence: str,
    lower: str,
    span: tuple[int, int],
    price: float,
    current_price: float | None,
) -> float:
    score = 1.0
    if "목표가" in lower or "price target" in lower or "target price" in lower:
        score += 2.0
    if any(cue in lower for cue in ("타겟", "target", "도달", "레벨", "간다", "까지")):
        score += 1.0
    if any(cue in lower for cue in CURRENT_PRICE_CUES):
        cue_index = min((lower.find(cue) for cue in CURRENT_PRICE_CUES if cue in lower), default=-1)
        if cue_index >= 0 and abs(cue_index - span[0]) <= 8:
            score -= 1.0
    if current_price is not None and current_price > 0 and abs(price - float(current_price)) / float(current_price) >= 0.05:
        score += 0.5
    if len(sentence) <= 160:
        score += 0.2
    return score


def _infer_direction(price: float, current_price: float | None, lower: str) -> str:
    if current_price is not None and current_price > 0:
        return "UP" if price >= float(current_price) else "DOWN"
    if any(cue in lower for cue in BEARISH_CUES):
        return "DOWN"
    return "UP"


def _coerce_target_dict(item: PriceTarget | dict[str, Any]) -> dict[str, Any]:
    if isinstance(item, PriceTarget):
        return {
            "target_price": item.target_price,
            "currency": item.currency,
            "confidence": item.confidence,
            "direction": item.direction,
            "time_horizon": item.time_horizon,
            "reasoning": item.reasoning,
            "evidence": list(item.evidence),
        }
    return dict(item)
