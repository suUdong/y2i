from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Any

from .evaluation import ranking_spearman
from .models import FundamentalSnapshot, MasterOpinion
from .reporting import render_fundamentals_lines, render_master_line
from .research import render_cross_video_ranking_text


@dataclass(slots=True)
class RunContext:
    """Execution context for one comparison run."""

    run_id: str
    today: str
    output_dir: Path
    window_days: int


def quality_scorecard(rows: list[dict], validation: dict[str, Any], ranking: list) -> dict[str, float]:
    """Build a simple quality scorecard for a channel artifact."""
    total_videos = len(rows)
    actionable_videos = sum(1 for row in rows if row["should_analyze_stocks"])
    transcript_backed = sum(1 for row in rows if (row["transcript_language"] or "").startswith("cache") or row["transcript_language"] not in {None, "metadata_fallback"})
    holding_days = [
        (date.fromisoformat(item["exit_date"]) - date.fromisoformat(item["entry_date"])).days
        for item in validation.get(f"top_{len(ranking)}", {}).get("positions", [])
    ]
    avg_holding_days = mean(holding_days) if holding_days else 0.0
    top3 = validation.get("top_3", {}).get("portfolio_return_pct", 0.0)
    spearman = ranking_spearman(ranking, validation) or 0.0
    ranking_predictive_power = 0.0 if not holding_days else round(max(0.0, min(100.0, 50.0 + top3 * 2.0 + spearman * 25.0)), 1)
    scores = {
        "transcript_coverage": round(min(100.0, (transcript_backed / total_videos) * 100 if total_videos else 0.0), 1),
        "actionable_density": round(min(100.0, (actionable_videos / total_videos) * 120 if total_videos else 0.0), 1),
        "ranking_predictive_power": ranking_predictive_power,
        "horizon_adequacy": round(min(100.0, (avg_holding_days / 20.0) * 100 if holding_days else 0.0), 1),
    }
    overall = round(mean(scores.values()) if scores else 0.0, 1)
    return {"overall": overall, **scores}


def save_channel_artifacts(
    slug: str,
    display_name: str,
    channel_url: str,
    rows: list[dict],
    ranking: list,
    validation: dict[str, Any],
    scorecard: dict[str, float],
    context: RunContext,
) -> tuple[Path, Path]:
    """Persist a per-channel JSON/TXT artifact pair."""
    payload = {
        "channel_slug": slug,
        "channel_name": display_name,
        "channel_url": channel_url,
        "generated_at": context.run_id,
        "window_days": context.window_days,
        "videos": rows,
        "cross_video_ranking": [item.to_dict() for item in ranking],
        "ranking_validation": validation,
        "quality_scorecard": scorecard,
    }
    json_path = context.output_dir / f"{slug}_30d_{context.run_id}.json"
    txt_path = context.output_dir / f"{slug}_30d_{context.run_id}.txt"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"{display_name} 최근 {context.window_days}일 분석 ({context.run_id})",
        f"채널: {channel_url}",
        "",
        render_cross_video_ranking_text(ranking),
        "",
        "[랭킹 검증]",
        f"종료일: {context.today}",
        f"top_1 수익률: {validation.get('top_1', {}).get('portfolio_return_pct', 0.0)}%",
        f"top_3 수익률: {validation.get('top_3', {}).get('portfolio_return_pct', 0.0)}%",
        f"top_{len(ranking)} 수익률: {validation.get(f'top_{len(ranking)}', {}).get('portfolio_return_pct', 0.0)}%",
        "",
        "[품질 점수]",
    ]
    for key, value in scorecard.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    for row in rows:
        lines.extend(
            [
                f"영상: {row['title']}",
                f"URL: {row['url']}",
                f"Published At: {row['published_at']}",
                f"Signal: {row['video_signal_class']} ({row['signal_score']:.1f}) | analyze={row['should_analyze_stocks']}",
                f"Reason: {row['reason']}",
            ]
        )
        if not row["stocks"]:
            lines.extend(["종목 결과: 없음", ""])
            continue
        lines.append("종목 결과:")
        for stock in row["stocks"]:
            lines.extend(
                [
                    f"- {stock['ticker']} | {stock['company_name']} | signal_strength={stock['signal_strength_score']} | 최종 {stock['final_verdict']} ({stock['final_score']:.1f})",
                    f"  evidence_source: {stock['evidence_source']}",
                    f"  evidence: {' | '.join(stock['evidence_snippets'])}",
                    f"  기본재무상태: {stock['basic_state']}",
                ]
            )
            snapshot = FundamentalSnapshot(**stock["fundamentals"])
            for fund_line in render_fundamentals_lines(snapshot)[:4]:
                lines.append(f"  {fund_line}")
            lines.append("  거장 한줄평:")
            for opinion in stock["master_opinions"]:
                lines.append(f"  {render_master_line(MasterOpinion(**opinion))}")
        lines.append("")
    txt_path.write_text("\n".join(lines), encoding="utf-8")
    return json_path, txt_path


def compare_channels(channel_payloads: dict[str, dict], context: RunContext) -> dict[str, Any]:
    """Summarize multiple per-channel payloads into one comparison artifact."""
    comparison = {
        "generated_at": context.run_id,
        "window_days": context.window_days,
        "channels": {},
    }
    if not channel_payloads:
        comparison["more_actionable_channel"] = None
        comparison["better_ranking_channel"] = None
        return comparison
    for slug, payload in channel_payloads.items():
        rows = payload["rows"]
        ranking = payload["ranking"]
        validation = payload["validation"]
        actionable_count = sum(1 for row in rows if row["should_analyze_stocks"])
        comparison["channels"][slug] = {
            "display_name": payload["display_name"],
            "total_videos": len(rows),
            "actionable_videos": actionable_count,
            "actionable_ratio": round(actionable_count / len(rows), 3) if rows else 0.0,
            "ranking_top_1_return_pct": validation.get("top_1", {}).get("portfolio_return_pct", 0.0),
            "ranking_top_3_return_pct": validation.get("top_3", {}).get("portfolio_return_pct", 0.0),
            "ranking_spearman": ranking_spearman(ranking, validation),
            "ranking_eval_positions": len(validation.get(f"top_{len(ranking)}", {}).get("positions", [])),
            "quality_scorecard": payload["scorecard"],
        }
    comparison["more_actionable_channel"] = max(
        comparison["channels"].items(),
        key=lambda item: item[1]["actionable_ratio"],
    )[0]
    comparison["better_ranking_channel"] = max(
        comparison["channels"].items(),
        key=lambda item: (
            item[1]["quality_scorecard"]["ranking_predictive_power"],
            item[1]["quality_scorecard"]["horizon_adequacy"],
            item[1]["ranking_eval_positions"],
        ),
    )[0]
    return comparison
