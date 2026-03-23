from __future__ import annotations


def stock_signal_strength(
    ticker: str,
    company_name: str | None,
    video_signal_score: float,
    mention_count: int,
    master_variance: float,
    evidence_snippets: list[str],
    evidence_source: str,
) -> float:
    evidence_score = _evidence_score(ticker, company_name, evidence_snippets)
    source_bonus = 8.0 if evidence_source in {"transcript_cache", "transcript_api", "cache"} else 2.0
    return round(
        min(
            100.0,
            video_signal_score * 0.55
            + min(mention_count * 1.6, 8.0)
            + master_variance * 1.1
            + evidence_score
            + source_bonus,
        ),
        1,
    )


def _evidence_score(ticker: str, company_name: str | None, evidence_snippets: list[str]) -> float:
    text = " ".join(evidence_snippets).lower()
    aliases = {
        alias.lower()
        for alias in [ticker, ticker.split(".")[0], company_name or "", *(company_name or "").split()]
        if alias
    }
    alias_hits = sum(1 for alias in aliases if alias and alias in text)
    density = min(len(evidence_snippets) * 2.5, 8.0)
    theme_bonus = 4.0 if any(keyword in text for keyword in ["로드맵", "실적", "수혜주", "병목", "변압기", "power", "network"]) else 0.0
    return min(16.0, density + alias_hits * 2.0 + theme_bonus)
