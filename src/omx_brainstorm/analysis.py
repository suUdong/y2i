from __future__ import annotations

from dataclasses import asdict

from .llm import LLMProvider
from .master_engine import validate_master_opinions
from .models import AnalysisScore, FundamentalSnapshot, MasterOpinion, StockAnalysis, TickerMention
from .prompts import ANALYSIS_SYSTEM, analysis_user_prompt
from .utils import chunk_text


class StockAnalyzer:
    def __init__(self, provider: LLMProvider, mode: str = "ralph"):
        self.provider = provider
        self.mode = mode

    def analyze(
        self,
        video_title: str,
        transcript_text: str,
        mention: TickerMention,
        fundamentals: FundamentalSnapshot,
    ) -> StockAnalysis:
        excerpt = chunk_text(transcript_text, max_chars=12000)[0]
        payload = self.provider.run_json(
            ANALYSIS_SYSTEM,
            analysis_user_prompt(
                video_title=video_title,
                transcript_excerpt=excerpt,
                ticker=mention.ticker,
                company_name=mention.company_name,
                fundamentals=asdict(fundamentals),
                mode=self.mode,
            ),
        )
        framework_scores = [
            AnalysisScore(
                framework=item["framework"],
                score=float(item["score"]),
                max_score=float(item["max_score"]),
                verdict=item["verdict"],
                summary=item["summary"],
                risks=list(item.get("risks", []) or []),
                citations=list(item.get("citations", []) or []),
                details=dict(item.get("details", {}) or {}),
            )
            for item in payload.get("framework_scores", [])
        ]
        master_opinions = [
            MasterOpinion(
                master=item["master"],
                verdict=item["verdict"],
                score=float(item.get("score", 0)),
                max_score=float(item.get("max_score", 100)),
                one_liner=item.get("one_liner", ""),
                rationale=list(item.get("rationale", []) or []),
                risks=list(item.get("risks", []) or []),
                citations=list(item.get("citations", []) or []),
            )
            for item in payload.get("master_opinions", [])
        ]
        validate_master_opinions(master_opinions)
        return StockAnalysis(
            ticker=payload.get("ticker", mention.ticker),
            company_name=payload.get("company_name") or mention.company_name or fundamentals.company_name,
            extracted_from_video=video_title,
            fundamentals=fundamentals,
            basic_state=payload.get("basic_state", "데이터 확인 필요"),
            basic_signal_summary=payload.get("basic_signal_summary", ""),
            basic_signal_verdict=payload.get("basic_signal_verdict", "WATCH"),
            master_opinions=master_opinions,
            thesis_summary=payload.get("thesis_summary", ""),
            framework_scores=framework_scores,
            total_score=float(payload.get("total_score", 0.0)),
            max_score=float(payload.get("max_score", 100.0)),
            final_verdict=payload.get("final_verdict", "WATCH"),
            invalidation_triggers=list(payload.get("invalidation_triggers", []) or []),
            citations=list(payload.get("citations", []) or []),
            raw_llm_payload=payload,
        )
