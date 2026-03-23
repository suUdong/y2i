from __future__ import annotations

from dataclasses import asdict
import logging
import uuid
from pathlib import Path

from .analysis import StockAnalyzer
from .extractors import HybridTickerExtractor
from .fundamentals import FundamentalsFetcher
from .llm import resolve_provider
from .models import VideoAnalysisReport, utc_now_iso
from .reporting import save_report
from .signal_gate import assess_video_signal
from .transcript_cache import TranscriptCache
from .youtube import TranscriptFetcher, YoutubeResolver

logger = logging.getLogger(__name__)


class OMXPipeline:
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

    def _analyze_resolved_video(self, video):
        metadata_text = " ".join(part for part in [video.title, video.description or "", " ".join(video.tags)] if part).strip()
        cached = self.transcript_cache.load(video.video_id)
        try:
            if cached and cached.get("transcript_text") and cached.get("source") != "metadata_fallback":
                segments = []
                language = f"cache:{cached.get('transcript_language') or 'unknown'}"
                transcript_text = cached["transcript_text"]
                transcript_source = cached.get("source", "cache")
            else:
                segments, language = self.fetcher.fetch(video.video_id)
                transcript_text = self.fetcher.join_segments(segments)
                transcript_source = "transcript_api"
                self.transcript_cache.save(video, transcript_text, language, transcript_source)
        except Exception as exc:
            logger.warning("Transcript fetch failed for %s: %s", video.video_id, exc)
            if cached and cached.get("transcript_text"):
                segments = []
                language = f"cache:{cached.get('transcript_language') or 'unknown'}"
                transcript_text = cached["transcript_text"]
                transcript_source = cached.get("source", "cache")
            else:
                segments = []
                language = "metadata_fallback"
                transcript_text = metadata_text
                transcript_source = "metadata_fallback"
                self.transcript_cache.save(video, transcript_text, language, transcript_source)
        analyses = []
        signal_assessment = assess_video_signal(
            video.title,
            transcript_text,
            description=video.description or "",
            tags=video.tags,
        )
        mentions = []
        analysis_text = transcript_text or metadata_text
        if signal_assessment.should_analyze_stocks:
            mentions = self.extractor.extract(video.title, analysis_text)
            for mention in mentions:
                snapshot = self.fundamentals.fetch(mention)
                analyses.append(self.analyzer.analyze(video.title, analysis_text, mention, snapshot))
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
        )
        return report, save_report(report, self.output_dir)

    def analyze_video(self, url_or_id: str):
        video = self.resolver.resolve_video(url_or_id)
        return self._analyze_resolved_video(video)

    def analyze_channel(self, channel_url: str, limit: int = 5):
        videos = self.resolver.resolve_channel_videos(channel_url, limit=limit)
        return [self._analyze_resolved_video(video) for video in videos]

    def analyze_channel_since(self, channel_url: str, days: int = 30, max_entries: int = 80):
        videos = self.resolver.resolve_channel_videos_since(channel_url, days=days, max_entries=max_entries)
        return [self._analyze_resolved_video(video) for video in videos]
