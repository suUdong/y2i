from __future__ import annotations

import re

from .llm import LLMProvider
from .macro_signals import indirect_macro_mentions
from .models import TickerMention
from .prompts import EXTRACTION_SYSTEM, extraction_user_prompt
from .utils import chunk_text, merge_mention, unique_preserve

TICKER_RE = re.compile(r"(?:\$|\b)([A-Z]{1,5})(?:\b)")
COMMON_FALSE_POSITIVES = {"GDP", "CPI", "AI", "ETF", "IRA", "CEO", "EPS", "PER", "FCF", "DCF", "FOMC", "WTI", "DXY"}


class HybridTickerExtractor:
    """Blend LLM extraction with regex hints and macro-derived indirect mentions."""

    def __init__(self, provider: LLMProvider, mode: str = "ralph"):
        self.provider = provider
        self.mode = mode

    def extract(self, video_title: str, transcript_text: str) -> list[TickerMention]:
        regex_candidates = [m.group(1) for m in TICKER_RE.finditer(transcript_text)]
        regex_candidates = [t for t in regex_candidates if t not in COMMON_FALSE_POSITIVES]
        chunks = chunk_text(transcript_text, max_chars=10000)
        mentions: dict[str, TickerMention] = {}
        for chunk in chunks[:3]:
            payload = self.provider.run_json(
                EXTRACTION_SYSTEM,
                extraction_user_prompt(
                    video_title=video_title,
                    transcript=chunk,
                    hint_tickers=unique_preserve(regex_candidates)[:20],
                    mode=self.mode,
                ),
            )
            for item in payload.get("mentions", []):
                ticker = str(item.get("ticker", "")).upper().strip()
                if not ticker:
                    continue
                mention = TickerMention(
                    ticker=ticker,
                    company_name=item.get("company_name"),
                    confidence=float(item.get("confidence", 0.0) or 0.0),
                    reason=item.get("reason", ""),
                    evidence=list(item.get("evidence", []) or []),
                )
                merge_mention(mentions, mention)
        for mention in indirect_macro_mentions(video_title, transcript_text):
            merge_mention(mentions, mention)
        return sorted(mentions.values(), key=lambda x: (-x.confidence, x.ticker))
