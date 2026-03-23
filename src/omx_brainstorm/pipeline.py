from __future__ import annotations

from dataclasses import asdict
import logging
import uuid
from pathlib import Path

from .analysis import StockAnalyzer
from .expert_interview import extract_expert_insights
from .extractors import HybridTickerExtractor
from .fundamentals import FundamentalsFetcher
from .llm import resolve_provider
from .macro_signals import extract_macro_insights
from .market_review import extract_market_review
from .models import VideoAnalysisReport, VideoType, utc_now_iso
from .reporting import save_report
from .signal_gate import assess_video_signal
from .transcript_cache import TranscriptCache
from .transcript_runtime import resolve_transcript_text
from .youtube import TranscriptFetcher, YoutubeResolver

logger = logging.getLogger(__name__)


class OMXPipeline:
    """Primary report-generation pipeline for one video or channel slice."""

    def __init__(self, provider_name: str, output_dir: Path, mode: str = "ralph", transcript_cache: TranscriptCache | None = None):
        self.provider_name = provider_name
        self.mode = mode
        self.provider = resolve_provider(provider_name)
        self.output_dir = output_dir
        self.resolver = YoutubeResolver()
        self.fetcher = TranscriptFetcher()
        self.transcript_cache = transcript_cache or TranscriptCache()
        self.fundamentals = FundamentalsFetcher()
        self.extractor = HybridTickerExtractor(self.provider, mode=mode)
        self.analyzer = StockAnalyzer(self.provider, mode=mode)

    def _run_stock_analysis(self, video_title, analysis_text, should_analyze):
        """Extract tickers and run per-stock analysis when signal warrants it."""
        if not should_analyze:
            return [], []
        mentions = self.extractor.extract(video_title, analysis_text)
        analyses = [
            self.analyzer.analyze(video_title, analysis_text, mention, self.fundamentals.fetch(mention))
            for mention in mentions
        ]
        return mentions, analyses

    def _analyze_resolved_video(self, video):
        transcript_text, language, transcript_source = self._resolve_transcript(video)
        metadata_text = " ".join(part for part in [video.title, video.description or "", " ".join(video.tags)] if part).strip()
        signal_assessment = assess_video_signal(
            video.title,
            transcript_text,
            description=video.description or "",
            tags=video.tags,
        )
        analysis_text = transcript_text or metadata_text
        video_type = VideoType(signal_assessment.video_type)
        should = signal_assessment.should_analyze_stocks

        # --- VideoType-based branching ---
        macro_insights = []
        market_review = None
        expert_insights = []

        if video_type in (VideoType.STOCK_PICK, VideoType.SECTOR):
            pass  # stock analysis only
        elif video_type == VideoType.MARKET_REVIEW:
            market_review = extract_market_review(video.title, analysis_text)
            macro_insights = market_review.macro_insights
        elif video_type == VideoType.EXPERT_INTERVIEW:
            expert_insights = extract_expert_insights(video.title, analysis_text, video.description or "")
            macro_insights = extract_macro_insights(video.title, analysis_text)
        else:
            # MACRO, NEWS_EVENT, OTHER
            macro_insights = extract_macro_insights(video.title, analysis_text)

        mentions, analyses = self._run_stock_analysis(video.title, analysis_text, should)

        self.transcript_cache.save(
            video=video,
            transcript_text=analysis_text,
            transcript_language=language,
            source=transcript_source,
            ticker_mentions=[asdict(item) for item in mentions],
        )
        report = VideoAnalysisReport(
            run_id=uuid.uuid4().hex[:10],
            created_at=utc_now_iso(),
            provider=self.provider_name,
            mode=self.mode,
            video=video,
            signal_assessment=signal_assessment,
            transcript_text=analysis_text,
            transcript_language=language,
            ticker_mentions=mentions,
            stock_analyses=analyses,
            macro_insights=macro_insights,
            market_review=market_review,
            expert_insights=expert_insights,
        )
        return report, save_report(report, self.output_dir)

    def _resolve_transcript(self, video) -> tuple[str, str, str]:
        """Resolve transcript text from cache, live fetch, or metadata fallback."""
        transcript_text, language, transcript_source, _cached = resolve_transcript_text(video, self.transcript_cache, self.fetcher, logger)
        return transcript_text, language, transcript_source

    def analyze_video(self, url_or_id: str):
        video = self.resolver.resolve_video(url_or_id)
        return self._analyze_resolved_video(video)

    def analyze_channel(self, channel_url: str, limit: int = 5):
        videos = self.resolver.resolve_channel_videos(channel_url, limit=limit)
        return [self._analyze_resolved_video(video) for video in videos]

    def analyze_channel_since(self, channel_url: str, days: int = 30, max_entries: int = 80):
        videos = self.resolver.resolve_channel_videos_since(channel_url, days=days, max_entries=max_entries)
        return [self._analyze_resolved_video(video) for video in videos]
