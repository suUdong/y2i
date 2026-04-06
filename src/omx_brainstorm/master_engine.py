from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from statistics import pstdev
from typing import Any

from .models import FundamentalSnapshot, MasterOpinion

_SKIP_SUFFIXES = ("_cap", "_threshold", "_rate", "_bonus", "_penalty")
_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "masters.toml"


@dataclass(slots=True)
class MasterConfig:
    name: str
    display: str
    focus: str
    key_metrics: list[str]
    base_score: float
    weights: dict[str, float]
    risks: list[str]
    templates: dict[str, str | list[str]]


def load_masters(path: Path | None = None) -> list[MasterConfig]:
    """Load master profiles from TOML config."""
    config_path = path or _DEFAULT_CONFIG_PATH
    with open(config_path, "rb") as f:
        data = tomllib.load(f)
    masters = []
    for entry in data.get("masters", []):
        weights_raw = entry.get("weights", {})
        weights = {k: float(v) for k, v in weights_raw.items()}
        masters.append(MasterConfig(
            name=entry["name"],
            display=entry.get("display", entry["name"]),
            focus=entry.get("focus", ""),
            key_metrics=list(entry.get("key_metrics", [])),
            base_score=float(entry.get("base_score", 50)),
            weights=weights,
            risks=list(entry.get("risks", {}).get("items", [])),
            templates=dict(entry.get("templates", {})),
        ))
    return masters


def build_opinion(config: MasterConfig, metrics: dict[str, Any]) -> MasterOpinion:
    """Generic scoring engine: base_score + weighted components - penalties."""
    score = config.base_score
    weights = config.weights

    # Standard weighted components
    for key, weight in weights.items():
        if any(key.endswith(suffix) for suffix in _SKIP_SUFFIXES):
            continue
        value = float(metrics.get(key, 0))
        component = value * weight
        cap = weights.get(f"{key}_cap")
        if cap is not None:
            component = min(component, float(cap))
        score += component

    # Valuation penalty
    val_threshold = weights.get("valuation_penalty_threshold")
    val_rate = weights.get("valuation_penalty_rate")
    if val_threshold is not None and val_rate is not None:
        forward_pe = float(metrics.get("forward_pe", 0))
        score -= max(forward_pe - val_threshold, 0) * val_rate

    # Leverage tiers (buffett-style)
    lev_low_threshold = weights.get("leverage_low_threshold")
    if lev_low_threshold is not None:
        leverage = float(metrics.get("leverage", 0))
        lev_mid_threshold = float(weights.get("leverage_mid_threshold", 120))
        if leverage < lev_low_threshold:
            score += float(weights.get("leverage_low_bonus", 0))
        elif leverage < lev_mid_threshold:
            score += float(weights.get("leverage_mid_bonus", 0))
        else:
            score += float(weights.get("leverage_high_penalty", 0))

    # Valuation tiers (buffett-style)
    val_low_threshold = weights.get("valuation_low_threshold")
    if val_low_threshold is not None:
        forward_pe = float(metrics.get("forward_pe", 0))
        val_mid_threshold = float(weights.get("valuation_mid_threshold", 30))
        if 0 < forward_pe < val_low_threshold:
            score += float(weights.get("valuation_low_bonus", 0))
        elif forward_pe < val_mid_threshold:
            score += float(weights.get("valuation_mid_bonus", 0))
        else:
            score += float(weights.get("valuation_high_penalty", 0))

    score = _clamp(score)
    verdict = master_verdict(score)

    # Format templates
    evidence = str(metrics.get("evidence", ""))
    template_vars = {**metrics, "verdict_kr": _verdict_kr(verdict)}
    one_liner = format_template(
        config.templates.get("one_liner", ""), template_vars,
    )
    rationale_templates = config.templates.get("rationale", [])
    rationale = [format_template(t, template_vars) for t in rationale_templates]

    # Auto-generate citations from key_metrics
    citations: list[str] = []
    for km in config.key_metrics:
        val = metrics.get(km)
        if val is not None:
            citations.append(f"fundamentals:{km}={val}")
            break
    if not citations:
        citations.append(f"fundamentals:base_score={config.base_score}")
    citations.append(f"evidence:{evidence}")

    return MasterOpinion(
        master=config.name,
        verdict=verdict,
        score=round(score, 1),
        max_score=100.0,
        one_liner=one_liner,
        rationale=rationale,
        risks=list(config.risks),
        citations=citations,
    )


def format_template(template: str, metrics: dict[str, Any]) -> str:
    """Format a template string with metrics, handling missing keys gracefully."""
    try:
        return template.format(**metrics)
    except (KeyError, ValueError, IndexError):
        # Fall back: replace known keys, leave unknown as-is
        result = template
        for key, value in metrics.items():
            try:
                result = result.replace(f"{{{key}}}", str(value))
                # Handle format specs like {revenue:.1f}
                for spec in [".1f", ".2f", ".0f", "d"]:
                    placeholder = f"{{{key}:{spec}}}"
                    if placeholder in result:
                        try:
                            result = result.replace(
                                placeholder, f"{float(value):{spec}}",
                            )
                        except (TypeError, ValueError):
                            result = result.replace(placeholder, str(value))
            except Exception:
                continue
        return result


def build_master_opinions(
    ticker: str,
    company_name: str | None,
    snapshot: FundamentalSnapshot,
    mention_count: int,
    video_title: str,
    video_signal_score: float,
    evidence_snippets: list[str],
    masters_config_path: Path | None = None,
) -> list[MasterOpinion]:
    """Build stock-specific opinions using config-driven masters. Backward-compatible."""
    evidence_snippets = [
        item.strip() for item in evidence_snippets if item and item.strip()
    ]
    primary_evidence = evidence_snippets[0] if evidence_snippets else video_title
    revenue = _pct(snapshot.revenue_growth)
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    momentum = _pct(snapshot.fifty_two_week_change)
    forward_pe = snapshot.forward_pe or 0.0
    leverage = snapshot.debt_to_equity or 0.0
    earnings_growth = _pct(snapshot.earnings_growth)
    theme_score = 1 if any(
        word in video_title.lower()
        for word in ["로드맵", "roadmap", "foundry", "gpu", "전력", "memory"]
    ) else 0

    metrics = {
        "ticker": ticker,
        "evidence": primary_evidence,
        "mention_count": mention_count,
        "mention": mention_count,
        "video_signal_score": video_signal_score,
        "signal": video_signal_score,
        "revenue": revenue,
        "margin": margin,
        "roe": roe,
        "momentum": momentum,
        "forward_pe": forward_pe,
        "leverage": leverage,
        "theme": theme_score,
        "theme_score": theme_score,
        "earnings_growth": earnings_growth,
    }

    masters = load_masters(masters_config_path)
    opinions = [build_opinion(config, metrics) for config in masters]
    return validate_master_opinions(opinions)


def master_variance_score(
    master_opinions: list[dict] | list[MasterOpinion],
) -> float:
    """Measure disagreement across master scores."""
    scores = []
    for item in master_opinions:
        if isinstance(item, MasterOpinion):
            scores.append(float(item.score))
        else:
            scores.append(float(item.get("score", 0.0)))
    if len(scores) < 2:
        return 0.0
    return round(pstdev(scores), 2)


def validate_master_opinions(
    master_opinions: list[MasterOpinion],
) -> list[MasterOpinion]:
    """Validate that each master opinion contains required non-boilerplate content."""
    one_liners = [
        item.one_liner.strip() for item in master_opinions if item.one_liner.strip()
    ]
    if len(one_liners) != len(master_opinions):
        raise ValueError("master one-liner missing")
    if len(set(one_liners)) != len(one_liners):
        raise ValueError("master one-liners must be unique per stock")
    for item in master_opinions:
        if not item.rationale:
            raise ValueError(f"{item.master} rationale missing")
        if not any(
            citation.startswith("fundamentals:") for citation in item.citations
        ):
            raise ValueError(f"{item.master} fundamentals citation missing")
        if not any(
            citation.startswith("evidence:") for citation in item.citations
        ):
            raise ValueError(f"{item.master} evidence citation missing")
    return master_opinions


def validate_cross_stock_master_quality(stocks: list[dict]) -> None:
    """Reject repeated identical master sentences across different stocks."""
    seen: dict[tuple[str, str], str] = {}
    for stock in stocks:
        ticker = stock.get("ticker", "unknown")
        for item in stock.get("master_opinions", []):
            master = item["master"] if isinstance(item, dict) else item.master
            sentence = (
                item["one_liner"] if isinstance(item, dict) else item.one_liner
            ).strip()
            if not sentence:
                raise ValueError(f"{ticker} missing master one-liner")
            key = (master, sentence)
            if key in seen and seen[key] != ticker:
                raise ValueError(
                    f"Repeated master sentence across stocks: {master} -> {sentence}"
                )
            seen[key] = ticker


def master_verdict(score: float) -> str:
    if score >= 80:
        return "BUY"
    if score >= 62:
        return "WATCH"
    return "REJECT"


def _clamp(score: float) -> float:
    return max(0.0, min(100.0, score))


def _pct(value: float | None) -> float:
    return 0.0 if value is None else value * 100.0


def _verdict_kr(verdict: str) -> str:
    return {"BUY": "매수", "WATCH": "관망", "REJECT": "회피"}.get(verdict, verdict)
