from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analysis_rows import analyze_resolved_videos_to_reports
from .app_config import AppConfig, StrategyConfig
from .comparison_rows import reports_to_comparison_rows
from .models import FundamentalSnapshot, TranscriptSegment, TickerMention, VideoInput
from .output_layout import build_run_output_dir
from .reporting import save_combined_dashboard
from .research import build_cross_video_ranking
from .transcript_cache import TranscriptCache
from .utils import write_json


@dataclass(slots=True)
class HarnessCase:
    video: VideoInput
    transcript_text: str
    transcript_language: str = "ko"
    transcript_source: str = "harness_offline"


@dataclass(slots=True)
class HarnessScenario:
    name: str
    description: str
    cases: list[HarnessCase]


class OfflineHarnessTranscriptFetcher:
    def __init__(self, cases: list[HarnessCase]):
        self._cases = {case.video.video_id: case for case in cases}

    def fetch_with_source(self, video_id: str):
        case = self._cases[video_id]
        return (
            [TranscriptSegment(start=0.0, duration=max(1.0, len(case.transcript_text) / 80.0), text=case.transcript_text)],
            case.transcript_language,
            case.transcript_source,
        )

    @staticmethod
    def join_segments(segments: list[TranscriptSegment]) -> str:
        return " ".join(segment.text for segment in segments)


class OfflineHarnessFundamentalsFetcher:
    def fetch(self, mention: TickerMention) -> FundamentalSnapshot:
        company_name = mention.company_name or mention.ticker
        return FundamentalSnapshot(
            ticker=mention.ticker,
            company_name=company_name,
            checked_at="2026-04-08T04:00:00+00:00",
            currency="USD",
            current_price=910.0,
            market_cap=2_250_000_000_000.0,
            trailing_pe=51.0,
            forward_pe=34.5,
            price_to_book=28.0,
            revenue_growth=0.41,
            earnings_growth=0.52,
            operating_margin=0.37,
            return_on_equity=0.46,
            debt_to_equity=32.0,
            fifty_two_week_change=1.08,
            data_source="harness_offline",
            notes=[],
            recommendation_key="buy",
            recommendation_mean=1.8,
            analyst_count=42,
            target_mean_price=980.0,
            target_median_price=1000.0,
            analyst_strong_buy=12,
            analyst_buy=21,
            analyst_hold=7,
            analyst_sell=2,
            analyst_strong_sell=0,
            sector="Technology",
            industry="Semiconductors",
        )

    def fetch_many(self, mentions: list[TickerMention], max_workers: int | None = None) -> dict[str, FundamentalSnapshot]:
        return {mention.ticker: self.fetch(mention) for mention in mentions}


def available_harness_scenarios() -> list[str]:
    return sorted(_scenario_map())


def describe_harness_scenarios() -> list[dict[str, str]]:
    return [
        {"name": item.name, "description": item.description}
        for item in sorted(_scenario_map().values(), key=lambda scenario: scenario.name)
    ]


def run_harness(
    *,
    output_dir: str | Path,
    scenario: str = "basic",
    provider_name: str = "mock",
    run_id: str | None = None,
) -> dict[str, Any]:
    scenario_payload = _scenario_map().get(scenario)
    if scenario_payload is None:
        raise ValueError(f"Unknown harness scenario: {scenario}")

    run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = build_run_output_dir(Path(output_dir) / "harness", run_id)
    config = AppConfig(
        provider=provider_name,
        output_dir=str(run_dir),
        strategy=StrategyConfig(video_workers=1, fundamentals_workers=1),
    )
    transcript_cache = TranscriptCache(run_dir / "cache" / "transcripts")
    transcript_fetcher = OfflineHarnessTranscriptFetcher(scenario_payload.cases)
    fundamentals_fetcher = OfflineHarnessFundamentalsFetcher()

    reports = analyze_resolved_videos_to_reports(
        [case.video for case in scenario_payload.cases],
        config=config,
        transcript_cache=transcript_cache,
        fetcher=transcript_fetcher,
        fundamentals_fetcher=fundamentals_fetcher,
        output_dir=run_dir,
        persist=True,
    )
    rows = reports_to_comparison_rows(reports)
    ranking = build_cross_video_ranking(rows)
    dashboard_path = save_combined_dashboard(reports, run_dir, label=f"{scenario}_dashboard") if reports else None
    ranking_path = run_dir / f"{scenario}_ranking.json"
    summary_path = run_dir / f"{scenario}_summary.json"
    summary_txt_path = run_dir / f"{scenario}_summary.txt"
    write_json(ranking_path, [item.to_dict() for item in ranking])

    video_type_counts = Counter(report.signal_assessment.video_type for report in reports)
    signal_class_counts = Counter(report.signal_assessment.video_signal_class for report in reports)
    stock_count = sum(len(report.stock_analyses) for report in reports)
    report_summaries = [
        {
            "video_id": report.video.video_id,
            "title": report.video.title,
            "video_type": report.signal_assessment.video_type,
            "signal_class": report.signal_assessment.video_signal_class,
            "signal_score": round(report.signal_assessment.signal_score, 1),
            "transcript_backed": report.transcript_backed,
            "transcript_language": report.transcript_language,
            "stock_count": len(report.stock_analyses),
            "tickers": [item.ticker for item in report.ticker_mentions],
        }
        for report in reports
    ]
    summary = {
        "scenario": scenario_payload.name,
        "description": scenario_payload.description,
        "provider": provider_name,
        "generated_at": run_id,
        "run_dir": str(run_dir),
        "video_count": len(reports),
        "stock_count": stock_count,
        "ranking_count": len(ranking),
        "top_ticker": ranking[0].ticker if ranking else None,
        "top_verdict": ranking[0].aggregate_verdict if ranking else None,
        "video_type_counts": dict(video_type_counts),
        "signal_class_counts": dict(signal_class_counts),
        "reports": report_summaries,
        "dashboard_path": str(dashboard_path) if dashboard_path else None,
        "ranking_path": str(ranking_path),
        "summary_path": str(summary_path),
        "summary_txt_path": str(summary_txt_path),
        "report_files": [],
    }
    write_json(summary_path, summary)
    summary_txt_path.write_text(_render_harness_summary(summary), encoding="utf-8")
    summary["report_files"] = sorted(path.name for path in run_dir.glob("*") if path.is_file())
    write_json(summary_path, summary)
    return summary


def _scenario_map() -> dict[str, HarnessScenario]:
    return {
        "basic": HarnessScenario(
            name="basic",
            description="Offline stock pick + market review + expert interview smoke run",
            cases=[
                HarnessCase(
                    video=VideoInput(
                        video_id="stockcase01",
                        title="엔비디아 주가 전망, 데이터센터 수혜주 점검",
                        url="https://youtube.com/watch?v=stockcase01",
                        published_at="20260408",
                        description="엔비디아와 데이터센터 CAPEX, 반도체 업황을 같이 다룬다.",
                        tags=["엔비디아", "데이터센터", "반도체"],
                    ),
                    transcript_text=(
                        "엔비디아가 아직 더 갈 수 있다고 본다. 데이터센터 수요가 강하고 AI 인프라 CAPEX가 늘어난다. "
                        "반도체 업황 회복과 GPU 증설, 데이터센터 투자 확대가 실적과 마진 개선으로 이어질 수 있다."
                    ),
                ),
                HarnessCase(
                    video=VideoInput(
                        video_id="macrocase01",
                        title="장마감 시황 | 금리와 환율, 코스피 흐름 점검",
                        url="https://youtube.com/watch?v=macrocase01",
                        published_at="20260408",
                        description="오늘 시황과 금리, 환율, 코스피 흐름을 정리한다.",
                        tags=["시황", "금리", "환율"],
                    ),
                    transcript_text=(
                        "오늘 코스피와 나스닥은 혼조였다. 기준금리 인하 기대와 환율 안정이 시장 심리에 영향을 줬다. "
                        "장마감 기준으로 외국인 수급은 둔화됐지만 에너지 가격과 채권 금리는 안정 흐름을 보였다."
                    ),
                ),
                HarnessCase(
                    video=VideoInput(
                        video_id="expertcase1",
                        title="반도체 인터뷰 | 김철수 한국증권 이사",
                        url="https://youtube.com/watch?v=expertcase1",
                        published_at="20260408",
                        description="HBM과 메모리 업황 회복을 다루는 인터뷰.",
                        tags=["인터뷰", "반도체", "HBM"],
                    ),
                    transcript_text=(
                        "김철수 한국증권 이사는 하반기 메모리 업황이 회복될 것으로 전망했다. "
                        "삼성전자와 SK하이닉스가 HBM 수요 증가의 수혜를 볼 것이라고 예상했다. "
                        "AI 서버 투자와 데이터센터 증설이 이어지면 반도체 투자심리도 개선될 수 있다고 봤다."
                    ),
                ),
            ],
        ),
        "metadata_fallback": HarnessScenario(
            name="metadata_fallback",
            description="Offline metadata-only fallback smoke run for transcript-missing downgrades",
            cases=[
                HarnessCase(
                    video=VideoInput(
                        video_id="fallback01x",
                        title="엔비디아 데이터센터 수혜주 체크",
                        url="https://youtube.com/watch?v=fallback01x",
                        published_at="20260408",
                        description="엔비디아와 데이터센터, 반도체 수혜주를 메타데이터에서만 언급한다.",
                        tags=["엔비디아", "데이터센터", "수혜주"],
                    ),
                    transcript_text="엔비디아 데이터센터 반도체 수혜주 전망 메타데이터 요약",
                    transcript_language="metadata_fallback",
                    transcript_source="metadata_fallback",
                ),
                HarnessCase(
                    video=VideoInput(
                        video_id="fallback02x",
                        title="시장 잡담과 투자 메모",
                        url="https://youtube.com/watch?v=fallback02x",
                        published_at="20260408",
                        description="금리와 시장 분위기 정도만 메타데이터에서 짧게 언급한다.",
                        tags=["시장", "금리"],
                    ),
                    transcript_text="시장 금리 분위기 메모",
                    transcript_language="metadata_fallback",
                    transcript_source="metadata_fallback",
                ),
            ],
        ),
    }


def _render_harness_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"Harness scenario: {summary['scenario']}",
        f"Description: {summary['description']}",
        f"Provider: {summary['provider']}",
        f"Generated at: {summary['generated_at']}",
        f"Run dir: {summary['run_dir']}",
        f"Videos: {summary['video_count']}",
        f"Stock analyses: {summary['stock_count']}",
        f"Ranking count: {summary['ranking_count']}",
        f"Top ticker: {summary['top_ticker'] or '-'}",
        f"Top verdict: {summary['top_verdict'] or '-'}",
        "",
        "[Video Types]",
    ]
    for key, value in sorted(summary.get("video_type_counts", {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "[Signal Classes]"])
    for key, value in sorted(summary.get("signal_class_counts", {}).items()):
        lines.append(f"- {key}: {value}")
    lines.extend(["", "[Per Video]"])
    for item in summary.get("reports", []):
        lines.append(
            f"- {item['video_id']} | {item['video_type']} | {item['signal_class']} | "
            f"transcript_backed={item['transcript_backed']} | stocks={item['stock_count']}"
        )
    lines.extend(
        [
            "",
            f"Dashboard: {summary.get('dashboard_path') or '-'}",
            f"Ranking JSON: {summary['ranking_path']}",
            f"Summary JSON: {summary['summary_path']}",
        ]
    )
    return "\n".join(lines) + "\n"
