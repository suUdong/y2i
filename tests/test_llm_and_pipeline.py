from dataclasses import asdict
import json
from pathlib import Path

from omx_brainstorm.analysis import StockAnalyzer
from omx_brainstorm.extractors import HybridTickerExtractor
from omx_brainstorm.fundamentals import FundamentalsFetcher
from omx_brainstorm.llm import LLMResponse, LLMProvider, MockProvider, extract_json_object
from omx_brainstorm.models import FundamentalSnapshot, TickerMention, TranscriptSegment, VideoInput
from omx_brainstorm.pipeline import OMXPipeline
from omx_brainstorm.transcript_cache import TranscriptCache


class DummyResolver:
    def resolve_video(self, url_or_id: str) -> VideoInput:
        return VideoInput(video_id="abc123def45", title="AI 반도체 투자와 메모리 로드맵", url="https://youtube.com/watch?v=abc123def45")

    def resolve_channel_videos(self, channel_url: str, limit: int = 5):
        return [self.resolve_video(channel_url)]


class DummyFetcher:
    def fetch(self, video_id: str, preferred_languages=None):
        text = (
            "엔비디아가 아직 더 갈 수 있다. 데이터센터 수요가 강하다. "
            "AI 반도체 투자와 메모리 로드맵을 점검한다. "
            "반도체 실적과 투자 전략, 메모리 사이클, 수혜주, 밸류체인, 장비와 소재, "
            "삼성전자와 SK하이닉스, 엔비디아, AI 서버 투자까지 모두 다룬다. "
        ) * 20
        return [TranscriptSegment(0, 1, text)], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class DummyFundamentals:
    def fetch(self, mention):
        return FundamentalSnapshot(
            ticker=mention.ticker,
            company_name="NVIDIA",
            currency="USD",
            current_price=900.0,
            market_cap=2_000_000_000_000.0,
            trailing_pe=45.0,
            forward_pe=32.0,
            price_to_book=20.0,
            revenue_growth=0.45,
            earnings_growth=0.52,
            operating_margin=0.36,
            return_on_equity=0.52,
            debt_to_equity=45.0,
            fifty_two_week_change=1.2,
            data_source="dummy",
        )


class FailingFundamentals:
    def fetch(self, mention):
        return FundamentalSnapshot(ticker=mention.ticker, company_name=mention.company_name, data_source="yfinance_error", notes=["fetch_error:RuntimeError"])


def test_extract_json_object_handles_wrapped_json():
    payload = extract_json_object("prefix {\"a\":1} suffix")
    assert payload == {"a": 1}


def test_hybrid_extractor_mock_provider_returns_mentions():
    mentions = HybridTickerExtractor(MockProvider()).extract("제목", "엔비디아가 아직 더 갈 수 있다")
    assert mentions
    assert mentions[0].ticker == "NVDA"


class EmptyExtractionProvider(LLMProvider):
    def run(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        return LLMResponse(provider="empty", text='{"mentions": []}')


def test_hybrid_extractor_adds_macro_indirect_mentions():
    mentions = HybridTickerExtractor(EmptyExtractionProvider()).extract(
        "금리 인하와 원화 강세 수혜주",
        "금리 인하, 원화 강세, 건설주, 증권주, 성장주를 점검한다.",
    )
    tickers = {item.ticker for item in mentions}
    assert "000720.KS" in tickers
    assert "006800.KS" in tickers


def test_fundamentals_fetcher_without_runtime_failure(monkeypatch):
    class DummyTicker:
        info = {"longName": "NVIDIA", "currentPrice": 100.0}
        fast_info = None

    class DummyYF:
        def Ticker(self, ticker):
            return DummyTicker()

    monkeypatch.setitem(__import__("sys").modules, "yfinance", DummyYF())
    snap = FundamentalsFetcher().fetch(TickerMention(ticker="NVDA"))
    assert snap.ticker == "NVDA"


def test_fundamentals_fetcher_uses_file_cache(monkeypatch, tmp_path):
    calls = {"count": 0}

    class DummyTicker:
        info = {"longName": "NVIDIA", "currentPrice": 100.0}
        fast_info = None

    class DummyYF:
        def Ticker(self, ticker):
            calls["count"] += 1
            return DummyTicker()

    monkeypatch.setitem(__import__("sys").modules, "yfinance", DummyYF())
    fetcher = FundamentalsFetcher(cache_root=tmp_path / "fundamentals")
    mention = TickerMention(ticker="NVDA")

    first = fetcher.fetch(mention)
    second = fetcher.fetch(mention)

    assert first.current_price == 100.0
    assert second.current_price == 100.0
    assert calls["count"] == 1


def test_fundamentals_fetcher_bounds_memory_cache(tmp_path):
    fetcher = FundamentalsFetcher(cache_root=tmp_path / "fundamentals", max_workers=1, memory_cache_size=2)
    for ticker in ("NVDA", "AAPL", "TSLA"):
        payload = {
            "cached_at": "2026-03-28T00:00:00+00:00",
            "snapshot": asdict(FundamentalSnapshot(ticker=ticker, company_name=ticker, data_source="cache")),
        }
        path = fetcher._cache_path(fetcher._cache_key(ticker))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
        fetcher._load_cache_entry(fetcher._cache_key(ticker))

    assert len(fetcher._memory_cache) == 2
    assert fetcher._cache_key("NVDA") not in fetcher._memory_cache


def test_fundamentals_fetcher_memory_cache_is_bounded_lru(tmp_path):
    fetcher = FundamentalsFetcher(cache_root=tmp_path / "fundamentals", memory_cache_max_entries=2)
    for ticker in ("AAA", "BBB", "CCC"):
        snapshot = FundamentalSnapshot(ticker=ticker, company_name=ticker, data_source="cache")
        fetcher._save_cache_entry(fetcher._cache_key(ticker), snapshot)

    assert len(fetcher._memory_cache) == 2
    assert fetcher._cache_key("AAA") not in fetcher._memory_cache

    fetcher._load_cache_entry(fetcher._cache_key("BBB"))
    fetcher._save_cache_entry(fetcher._cache_key("DDD"), FundamentalSnapshot(ticker="DDD", company_name="DDD", data_source="cache"))

    assert len(fetcher._memory_cache) == 2
    assert fetcher._cache_key("BBB") in fetcher._memory_cache
    assert fetcher._cache_key("DDD") in fetcher._memory_cache
    assert fetcher._cache_key("CCC") not in fetcher._memory_cache


def test_fundamentals_fetcher_bounds_in_memory_cache(monkeypatch, tmp_path):
    prices = {
        "NVDA": 100.0,
        "MU": 90.0,
        "AVGO": 110.0,
    }
    calls: list[str] = []

    class DummyTicker:
        def __init__(self, ticker: str):
            self.info = {"longName": ticker, "currentPrice": prices[ticker]}
            self.fast_info = None

    class DummyYF:
        def Ticker(self, ticker):
            calls.append(ticker)
            return DummyTicker(ticker)

    monkeypatch.setitem(__import__("sys").modules, "yfinance", DummyYF())
    fetcher = FundamentalsFetcher(cache_root=tmp_path / "fundamentals", max_memory_entries=2)

    fetcher.fetch(TickerMention(ticker="NVDA"))
    fetcher.fetch(TickerMention(ticker="MU"))
    fetcher.fetch(TickerMention(ticker="AVGO"))

    assert len(fetcher._memory_cache) == 2
    assert set(fetcher._memory_cache) == {"MU", "AVGO"}

    cached = fetcher.fetch(TickerMention(ticker="NVDA"))

    assert cached.current_price == 100.0
    assert calls == ["NVDA", "MU", "AVGO"]
    assert len(fetcher._memory_cache) == 2
    assert "NVDA" in fetcher._memory_cache
    assert "MU" not in fetcher._memory_cache


def test_stock_analyzer_mock_provider():
    fundamentals = DummyFundamentals().fetch(TickerMention(ticker="NVDA"))
    analysis = StockAnalyzer(MockProvider()).analyze("제목", "엔비디아가 아직 더 갈 수 있다", TickerMention(ticker="NVDA"), fundamentals)
    assert analysis.final_verdict == "BUY"
    assert analysis.basic_signal_verdict == "BUY"
    assert len(analysis.master_opinions) == 3


def test_pipeline_end_to_end(tmp_path: Path):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = DummyResolver()
    pipeline.fetcher = DummyFetcher()
    pipeline.fundamentals = DummyFundamentals()
    report, paths = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")
    assert report.ticker_mentions[0].ticker == "NVDA"
    assert report.stock_analyses[0].basic_state
    assert report.stock_analyses[0].master_opinions[0].master == "druckenmiller"
    assert paths[0].exists()
    assert paths[1].exists()
    assert paths[2].exists()


class FailingFetcher:
    def fetch(self, video_id: str, preferred_languages=None):
        raise RuntimeError("blocked")

    def join_segments(self, segments):
        return ""


class EmptyTranscriptFetcher:
    def fetch(self, video_id: str, preferred_languages=None):
        return [], "ko"

    def join_segments(self, segments):
        return ""


class MetadataResolver(DummyResolver):
    def resolve_video(self, url_or_id: str) -> VideoInput:
        return VideoInput(
            video_id="abc123def45",
            title="AI 전력 인프라 수혜주는?",
            url="https://youtube.com/watch?v=abc123def45",
            description="AI 데이터센터 전력 인프라와 HD현대일렉트릭, 효성중공업을 다룬다.",
            tags=["전력인프라", "hd현대일렉트릭", "효성중공업", "투자 분석"],
            published_at="20260317",
        )


class GenericTranscriptFetcher:
    def fetch(self, video_id: str, preferred_languages=None):
        return [TranscriptSegment(0, 1, "이번 영상은 업황과 수급만 간단히 본다.")], "ko"

    def join_segments(self, segments):
        return " ".join(s.text for s in segments)


class MetadataAwareExtractionProvider(LLMProvider):
    def run(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        payload = {"mentions": []}
        if "hd현대일렉트릭" in user_prompt.lower():
            payload = {
                "mentions": [
                    {
                        "ticker": "267260.KS",
                        "company_name": "HD Hyundai Electric",
                        "confidence": 0.88,
                        "reason": "메타데이터에서 HD현대일렉트릭을 명시적으로 언급",
                        "evidence": ["HD현대일렉트릭"],
                    }
                ]
            }
        return LLMResponse(provider="metadata-aware", text=__import__("json").dumps(payload, ensure_ascii=False))


def test_pipeline_uses_metadata_fallback_when_transcript_fetch_fails(tmp_path: Path):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = MetadataResolver()
    pipeline.fetcher = FailingFetcher()
    pipeline.fundamentals = DummyFundamentals()

    report, paths = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert report.signal_assessment.video_signal_class != "NOISE"
    assert report.transcript_language == "metadata_fallback"
    assert "HD현대일렉트릭" in report.transcript_text
    assert paths[0].exists()


def test_pipeline_uses_metadata_to_extract_tickers_when_transcript_is_generic(tmp_path: Path):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.provider = MetadataAwareExtractionProvider()
    pipeline.extractor = HybridTickerExtractor(pipeline.provider, mode=pipeline.mode)
    pipeline.resolver = MetadataResolver()
    pipeline.fetcher = GenericTranscriptFetcher()
    pipeline.fundamentals = DummyFundamentals()

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert any(mention.ticker == "267260.KS" for mention in report.ticker_mentions)


def test_pipeline_uses_cached_transcript_when_fetch_fails(tmp_path: Path):
    cache = TranscriptCache(tmp_path / "cache")
    video = MetadataResolver().resolve_video("https://youtube.com/watch?v=abc123def45")
    cache.save(
        video=video,
        transcript_text="엔비디아가 아직 더 갈 수 있다. 데이터센터 수요가 강하다.",
        transcript_language="ko",
        source="transcript_cache",
        ticker_mentions=[{"ticker": "NVDA", "company_name": "NVIDIA", "evidence": ["엔비디아가 아직 더 갈 수 있다."]}],
    )

    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=cache)
    pipeline.resolver = MetadataResolver()
    pipeline.fetcher = FailingFetcher()
    pipeline.fundamentals = DummyFundamentals()

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert report.transcript_language.startswith("cache")
    assert "엔비디아" in report.transcript_text


def test_pipeline_uses_metadata_fallback_when_transcript_is_empty(tmp_path: Path):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = MetadataResolver()
    pipeline.fetcher = EmptyTranscriptFetcher()
    pipeline.fundamentals = DummyFundamentals()

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert report.transcript_language == "metadata_fallback"
    assert "HD현대일렉트릭" in report.transcript_text


def test_pipeline_gracefully_handles_fundamentals_failure(tmp_path: Path):
    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = DummyResolver()
    pipeline.fetcher = DummyFetcher()
    pipeline.fundamentals = FailingFundamentals()

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert report.stock_analyses
    assert report.stock_analyses[0].fundamentals.notes


def test_pipeline_downgrades_actionable_when_no_tickers_remain(tmp_path: Path, monkeypatch):
    from omx_brainstorm.models import VideoSignalAssessment

    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = DummyResolver()
    pipeline.fetcher = GenericTranscriptFetcher()
    pipeline.extractor = type("EmptyExtractor", (), {"extract": lambda self, title, text: []})()

    monkeypatch.setattr(
        "omx_brainstorm.pipeline.assess_video_signal",
        lambda *args, **kwargs: VideoSignalAssessment(
            signal_score=76.0,
            video_signal_class="ACTIONABLE",
            should_analyze_stocks=True,
            reason="pre-gate actionable",
            video_type="OTHER",
            metrics={"test": True},
        ),
    )

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=abc123def45")

    assert report.signal_assessment.video_signal_class == "LOW_SIGNAL"
    assert report.signal_assessment.should_analyze_stocks is False
    assert report.signal_assessment.skip_reason
    assert report.ticker_mentions == []
    assert report.stock_analyses == []


def test_pipeline_cached_metadata_fallback_does_not_analyze_macro_only_video(tmp_path: Path):
    class MacroOnlyResolver:
        def resolve_video(self, url_or_id: str) -> VideoInput:
            return VideoInput(
                video_id="metaonly12345",
                title="[LIVE] 이란 전쟁은 안 끝나고, M7은 끝났다?",
                url="https://youtube.com/watch?v=metaonly12345",
                description="중동 위기와 시장 충격을 다룬다.",
                tags=["전쟁", "중동", "M7"],
            )

    pipeline = OMXPipeline(provider_name="mock", output_dir=tmp_path, transcript_cache=TranscriptCache(tmp_path / "cache"))
    pipeline.resolver = MacroOnlyResolver()
    pipeline.fetcher = FailingFetcher()
    pipeline.fundamentals = DummyFundamentals()

    report, _ = pipeline.analyze_video("https://youtube.com/watch?v=metaonly12345")

    assert report.transcript_language == "metadata_fallback"
    assert report.signal_assessment.video_signal_class in {"NOISE", "LOW_SIGNAL", "SECTOR_ONLY"}
    assert report.signal_assessment.should_analyze_stocks is False
    assert report.ticker_mentions == []
    assert report.stock_analyses == []
