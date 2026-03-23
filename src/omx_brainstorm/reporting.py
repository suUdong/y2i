from __future__ import annotations

from pathlib import Path

from .models import ExpertInsight, FundamentalSnapshot, MacroInsight, MarketReviewSummary, MasterOpinion, VideoAnalysisReport
from .utils import ensure_dir, write_json


def save_report(report: VideoAnalysisReport, output_dir: Path) -> tuple[Path, Path, Path]:
    ensure_dir(output_dir)
    stem = f"{report.video.video_id}_{report.run_id}"
    json_path = output_dir / f"{stem}.json"
    md_path = output_dir / f"{stem}.md"
    txt_path = output_dir / f"{stem}.txt"
    write_json(json_path, report.to_dict())
    markdown = render_markdown(report)
    text = render_text(report)
    md_path.write_text(markdown, encoding="utf-8")
    txt_path.write_text(text, encoding="utf-8")
    return json_path, md_path, txt_path


def save_combined_dashboard(reports: list[VideoAnalysisReport], output_dir: Path, label: str = "dashboard") -> Path:
    """Render and save a combined dashboard for multiple reports."""
    ensure_dir(output_dir)
    from .models import utc_now_iso
    timestamp = utc_now_iso().replace(":", "").replace("-", "")[:15]
    md_path = output_dir / f"{label}_{timestamp}.md"
    md_path.write_text(render_combined_dashboard(reports), encoding="utf-8")
    return md_path


def render_markdown(report: VideoAnalysisReport) -> str:
    lines = [
        f"# OMX 분석 리포트 - {report.video.title}",
        "",
        f"- Run ID: `{report.run_id}`",
        f"- Provider: `{report.provider}`",
        f"- Mode: `{report.mode}`",
        f"- Video: {report.video.url}",
        f"- Transcript Language: {report.transcript_language or 'unknown'}",
        f"- Signal Class: `{report.signal_assessment.video_signal_class}` ({report.signal_assessment.signal_score:.1f})",
        f"- Should Analyze Stocks: `{report.signal_assessment.should_analyze_stocks}`",
        f"- Signal Reason: {report.signal_assessment.reason}",
        "",
        "## 추출 종목",
    ]
    for mention in report.ticker_mentions:
        lines.extend(
            [
                f"- **{mention.ticker}** ({mention.company_name or 'unknown'}) / confidence={mention.confidence:.2f}",
                f"  - reason: {mention.reason}",
                f"  - evidence: {', '.join(mention.evidence) if mention.evidence else '-'}",
            ]
        )
    lines.append("")
    lines.append("## 종목별 분석")
    for stock in report.stock_analyses:
        lines.extend(
            [
                f"### {stock.ticker} - {stock.final_verdict} ({stock.total_score:.1f}/{stock.max_score:.1f})",
                "",
                f"- 회사명: {stock.company_name or 'unknown'}",
                f"- 기본 재무 상태: {stock.basic_state}",
                f"- 기본 평가: {stock.basic_signal_verdict}",
                f"- 기본 요약: {stock.basic_signal_summary}",
                f"- Thesis: {stock.thesis_summary}",
                f"- Invalidation: {', '.join(stock.invalidation_triggers) if stock.invalidation_triggers else '-'}",
                "",
                "#### 현재 기본 지표",
                "",
                render_fundamentals_markdown(stock.fundamentals),
                "",
                "#### 거장 한줄평",
                "",
                "| Master | Verdict | Score | One-liner |",
                "|---|---|---:|---|",
            ]
        )
        for opinion in stock.master_opinions:
            lines.append(
                f"| {opinion.master} | {opinion.verdict} | {opinion.score:.1f}/{opinion.max_score:.1f} | {opinion.one_liner} |"
            )
        lines.extend(["", "#### 프레임워크 점수", "", "| Framework | Score | Verdict | Summary |", "|---|---:|---|---|"])
        for item in stock.framework_scores:
            lines.append(f"| {item.framework} | {item.score:.1f}/{item.max_score:.1f} | {item.verdict} | {item.summary} |")
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_text(report: VideoAnalysisReport) -> str:
    lines = [
        f"OMX 분석 리포트 | {report.video.title}",
        f"Run ID: {report.run_id}",
        f"Provider: {report.provider}",
        f"Mode: {report.mode}",
        f"Video: {report.video.url}",
        f"Signal: {report.signal_assessment.video_signal_class} ({report.signal_assessment.signal_score:.1f})",
        f"Signal analyze flag: {report.signal_assessment.should_analyze_stocks}",
        f"Signal reason: {report.signal_assessment.reason}",
        "",
        "[추출 종목]",
    ]
    for mention in report.ticker_mentions:
        lines.append(f"- {mention.ticker} ({mention.company_name or 'unknown'}) conf={mention.confidence:.2f} | {mention.reason}")
    lines.append("")
    lines.append("[종목별 결과]")
    for stock in report.stock_analyses:
        invalidations = [f"- {item}" for item in stock.invalidation_triggers] if stock.invalidation_triggers else ["- 없음"]
        lines.extend(
            [
                "",
                f"{stock.ticker} | {stock.company_name or 'unknown'}",
                f"최종판정: {stock.final_verdict} ({stock.total_score:.1f}/{stock.max_score:.1f})",
                f"기본재무상태: {stock.basic_state}",
                f"기본평가: {stock.basic_signal_verdict}",
                f"기본지표요약: {stock.basic_signal_summary}",
                f"투자테제: {stock.thesis_summary}",
                "현재 기본 지표:",
                *render_fundamentals_lines(stock.fundamentals),
                "거장 한줄평:",
                *[render_master_line(opinion) for opinion in stock.master_opinions],
                "무효화 조건:",
                *invalidations,
            ]
        )
    return "\n".join(lines).strip() + "\n"


def render_fundamentals_markdown(snapshot: FundamentalSnapshot) -> str:
    rows = [
        ("Current Price", format_number(snapshot.current_price, snapshot.currency)),
        ("checked_at", snapshot.checked_at or "-"),
        ("Market Cap", format_large_number(snapshot.market_cap, snapshot.currency)),
        ("Trailing PE", format_ratio(snapshot.trailing_pe)),
        ("Forward PE", format_ratio(snapshot.forward_pe)),
        ("Price/Book", format_ratio(snapshot.price_to_book)),
        ("Revenue Growth", format_pct(snapshot.revenue_growth)),
        ("Earnings Growth", format_pct(snapshot.earnings_growth)),
        ("Operating Margin", format_pct(snapshot.operating_margin)),
        ("ROE", format_pct(snapshot.return_on_equity)),
        ("Debt/Equity", format_ratio(snapshot.debt_to_equity)),
        ("52W Change", format_pct(snapshot.fifty_two_week_change)),
        ("Data Source", snapshot.data_source or "-"),
    ]
    lines = ["| Metric | Value |", "|---|---|"]
    for key, value in rows:
        lines.append(f"| {key} | {value} |")
    if snapshot.notes:
        lines.append(f"| Notes | {', '.join(snapshot.notes)} |")
    return "\n".join(lines)


def render_fundamentals_lines(snapshot: FundamentalSnapshot) -> list[str]:
    return [
        f"- 현재가: {format_number(snapshot.current_price, snapshot.currency)}",
        f"- checked_at: {snapshot.checked_at or '-'}",
        f"- 시가총액: {format_large_number(snapshot.market_cap, snapshot.currency)}",
        f"- Trailing PE: {format_ratio(snapshot.trailing_pe)} / Forward PE: {format_ratio(snapshot.forward_pe)}",
        f"- P/B: {format_ratio(snapshot.price_to_book)} / ROE: {format_pct(snapshot.return_on_equity)}",
        f"- 매출성장률: {format_pct(snapshot.revenue_growth)} / 이익성장률: {format_pct(snapshot.earnings_growth)}",
        f"- 영업이익률: {format_pct(snapshot.operating_margin)} / 부채비율(D/E): {format_ratio(snapshot.debt_to_equity)}",
        f"- 52주 변화율: {format_pct(snapshot.fifty_two_week_change)}",
        f"- 데이터소스: {snapshot.data_source or '-'}",
        f"- 비고: {', '.join(snapshot.notes) if snapshot.notes else '-'}",
    ]


def render_master_line(opinion: MasterOpinion) -> str:
    return f"- {opinion.master}: {opinion.verdict} ({opinion.score:.1f}/{opinion.max_score:.1f}) | {opinion.one_liner}"


def format_pct(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:.1f}%"


def format_ratio(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f}"


def format_number(value: float | None, currency: str | None) -> str:
    if value is None:
        return "-"
    prefix = f"{currency} " if currency else ""
    return f"{prefix}{value:,.2f}"


def format_large_number(value: float | None, currency: str | None) -> str:
    if value is None:
        return "-"
    prefix = f"{currency} " if currency else ""
    if abs(value) >= 1_000_000_000:
        return f"{prefix}{value / 1_000_000_000:.2f}B"
    if abs(value) >= 1_000_000:
        return f"{prefix}{value / 1_000_000:.2f}M"
    return f"{prefix}{value:,.0f}"


# --- Combined Dashboard ---

def render_combined_dashboard(reports: list[VideoAnalysisReport]) -> str:
    """Render a unified dashboard combining stock analyses, macro signals, and expert insights."""
    lines = ["# OMX 통합 대시보드", ""]

    # 1. Macro Overview
    all_macro = _collect_macro_insights(reports)
    if all_macro:
        lines.append("## 매크로 시그널 요약")
        lines.append("")
        lines.append("| 지표 | 방향 | 센티먼트 | 신뢰도 | 수혜 섹터 |")
        lines.append("|---|---|---|---:|---|")
        for insight in all_macro:
            sectors = ", ".join(insight.beneficiary_sectors[:3]) if insight.beneficiary_sectors else "-"
            lines.append(f"| {insight.label} | {insight.direction} | {insight.sentiment} | {insight.confidence:.2f} | {sectors} |")
        lines.append("")

    # 2. Market Review (if any)
    reviews = [r.market_review for r in reports if r.market_review is not None]
    if reviews:
        lines.append("## 시장 리뷰")
        lines.append("")
        for review in reviews:
            lines.append(f"**시장 방향성:** {review.direction}")
            if review.indices:
                for idx in review.indices:
                    lines.append(f"- {idx['name']}: {idx['direction']} {idx.get('detail', '')}")
            if review.risk_events:
                lines.append(f"- 리스크: {', '.join(review.risk_events[:5])}")
            lines.append("")

    # 3. Expert Insights (if any)
    all_experts = [ei for r in reports for ei in r.expert_insights]
    if all_experts:
        lines.append("## 전문가 인사이트")
        lines.append("")
        for expert in all_experts:
            aff = f" ({expert.affiliation})" if expert.affiliation else ""
            lines.append(f"### {expert.expert_name}{aff}")
            lines.append(f"- 주제: {expert.topic}")
            lines.append(f"- 센티먼트: {expert.sentiment}")
            if expert.structured_claims:
                lines.append("- 구조화 주장:")
                for sc in expert.structured_claims[:5]:
                    direction_tag = f"[{sc.direction}]" if sc.direction != "NEUTRAL" else ""
                    lines.append(f"  - {direction_tag} {sc.claim} (신뢰도: {sc.confidence:.0%})")
                    if sc.reasoning:
                        lines.append(f"    - 근거: {sc.reasoning}")
            elif expert.key_claims:
                lines.append("- 핵심 주장:")
                for claim in expert.key_claims[:3]:
                    lines.append(f"  - {claim}")
            if expert.mentioned_tickers:
                lines.append(f"- 언급 종목: {', '.join(expert.mentioned_tickers)}")
            lines.append("")

    # 4. Stock Analysis Summary
    all_stocks = [(r.video.title, s) for r in reports for s in r.stock_analyses]
    if all_stocks:
        lines.append("## 종목 분석 요약")
        lines.append("")
        lines.append("| 종목 | 회사명 | 최종판정 | 점수 | 기본평가 | 출처영상 |")
        lines.append("|---|---|---|---:|---|---|")
        seen: set[str] = set()
        for video_title, stock in sorted(all_stocks, key=lambda x: -x[1].total_score):
            if stock.ticker in seen:
                continue
            seen.add(stock.ticker)
            title_short = video_title[:25] + "..." if len(video_title) > 25 else video_title
            lines.append(
                f"| {stock.ticker} | {stock.company_name or '-'} "
                f"| {stock.final_verdict} | {stock.total_score:.1f} "
                f"| {stock.basic_signal_verdict} | {title_short} |"
            )
        lines.append("")

        # Detailed per-stock master opinions
        lines.append("## 종목별 거장 한줄평")
        lines.append("")
        seen_detail: set[str] = set()
        for _title, stock in sorted(all_stocks, key=lambda x: -x[1].total_score):
            if stock.ticker in seen_detail:
                continue
            seen_detail.add(stock.ticker)
            lines.append(f"### {stock.ticker} - {stock.company_name or '-'}")
            lines.append(f"- 기본 상태: {stock.basic_state}")
            lines.append(f"- 투자 테제: {stock.thesis_summary}")
            lines.append("")
            if stock.master_opinions:
                lines.append("| Master | Verdict | Score | One-liner |")
                lines.append("|---|---|---:|---|")
                for op in stock.master_opinions:
                    lines.append(f"| {op.master} | {op.verdict} | {op.score:.1f}/{op.max_score:.1f} | {op.one_liner} |")
                lines.append("")
            if stock.invalidation_triggers:
                lines.append(f"- 무효화 조건: {', '.join(stock.invalidation_triggers)}")
                lines.append("")

    # 5. Video source summary
    lines.append("## 분석 영상 목록")
    lines.append("")
    lines.append("| 영상 | 유형 | Signal | 분석종목 수 |")
    lines.append("|---|---|---|---:|")
    for r in reports:
        title_short = r.video.title[:30] + "..." if len(r.video.title) > 30 else r.video.title
        lines.append(
            f"| {title_short} | {r.signal_assessment.video_type} "
            f"| {r.signal_assessment.video_signal_class} ({r.signal_assessment.signal_score:.0f}) "
            f"| {len(r.stock_analyses)} |"
        )
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _collect_macro_insights(reports: list[VideoAnalysisReport]) -> list[MacroInsight]:
    """Deduplicate and collect macro insights across reports, keeping highest confidence per indicator."""
    best: dict[str, MacroInsight] = {}
    for r in reports:
        for insight in r.macro_insights:
            existing = best.get(insight.indicator)
            if existing is None or insight.confidence > existing.confidence:
                best[insight.indicator] = insight
    return sorted(best.values(), key=lambda x: -x.confidence)
