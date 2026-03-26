from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timezone
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


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    for fmt in ("%Y%m%dT%H%M%SZ", "%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _is_transcript_backed(language: str | None) -> bool:
    return (language or "").startswith("cache") or language not in {None, "", "metadata_fallback"}


def summarize_channel_run(rows: list[dict[str, Any]]) -> dict[str, Any]:
    signal_breakdown: Counter[str] = Counter()
    skip_reason_counts: Counter[str] = Counter()
    transcript_backed_videos = 0
    metadata_fallback_videos = 0
    latest_published_at = ""
    latest_published_ts: datetime | None = None

    for row in rows:
        signal_breakdown[row.get("video_signal_class", "UNKNOWN")] += 1

        transcript_language = row.get("transcript_language")
        if transcript_language == "metadata_fallback":
            metadata_fallback_videos += 1
        elif _is_transcript_backed(transcript_language):
            transcript_backed_videos += 1

        if not row.get("should_analyze_stocks"):
            skip_reason = (row.get("skip_reason") or row.get("reason") or "").strip()
            if skip_reason:
                skip_reason_counts[skip_reason] += 1

        published_at = row.get("published_at", "")
        published_ts = _parse_timestamp(published_at)
        if published_ts is not None and (latest_published_ts is None or published_ts > latest_published_ts):
            latest_published_ts = published_ts
            latest_published_at = published_at

    analyzable_videos = sum(1 for row in rows if row.get("should_analyze_stocks"))
    strict_actionable_videos = sum(1 for row in rows if row.get("video_signal_class") == "ACTIONABLE")
    skipped_videos = len(rows) - analyzable_videos
    return {
        "actionable_videos": analyzable_videos,
        "analyzable_videos": analyzable_videos,
        "strict_actionable_videos": strict_actionable_videos,
        "skipped_videos": skipped_videos,
        "transcript_backed_videos": transcript_backed_videos,
        "metadata_fallback_videos": metadata_fallback_videos,
        "latest_published_at": latest_published_at,
        "signal_breakdown": dict(signal_breakdown),
        "top_skip_reasons": [
            {"reason": reason, "count": count}
            for reason, count in skip_reason_counts.most_common(3)
        ],
    }


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
        comparison["pipeline_summary"] = {
            "total_channels": 0,
            "total_videos": 0,
            "actionable_videos": 0,
            "analyzable_videos": 0,
            "strict_actionable_videos": 0,
            "skipped_videos": 0,
            "transcript_backed_videos": 0,
            "metadata_fallback_videos": 0,
            "latest_published_at": "",
            "signal_breakdown": {},
            "top_skip_reasons": [],
        }
        return comparison

    aggregate_signal_breakdown: Counter[str] = Counter()
    aggregate_skip_reasons: Counter[str] = Counter()
    latest_published_at = ""
    latest_published_ts: datetime | None = None
    total_videos = 0
    analyzable_videos = 0
    strict_actionable_videos = 0
    skipped_videos = 0
    transcript_backed_videos = 0
    metadata_fallback_videos = 0
    for slug, payload in channel_payloads.items():
        rows = payload["rows"]
        ranking = payload["ranking"]
        validation = payload["validation"]
        channel_run = summarize_channel_run(rows)
        analyzable_count = channel_run["analyzable_videos"]
        total_videos += len(rows)
        analyzable_videos += channel_run["analyzable_videos"]
        strict_actionable_videos += channel_run["strict_actionable_videos"]
        skipped_videos += channel_run["skipped_videos"]
        transcript_backed_videos += channel_run["transcript_backed_videos"]
        metadata_fallback_videos += channel_run["metadata_fallback_videos"]
        aggregate_signal_breakdown.update(channel_run["signal_breakdown"])
        for item in channel_run["top_skip_reasons"]:
            aggregate_skip_reasons[item["reason"]] += item["count"]

        channel_latest_ts = _parse_timestamp(channel_run["latest_published_at"])
        if channel_latest_ts is not None and (latest_published_ts is None or channel_latest_ts > latest_published_ts):
            latest_published_ts = channel_latest_ts
            latest_published_at = channel_run["latest_published_at"]
        comparison["channels"][slug] = {
            "display_name": payload["display_name"],
            "total_videos": len(rows),
            "actionable_videos": analyzable_count,
            "analyzable_videos": analyzable_count,
            "strict_actionable_videos": channel_run["strict_actionable_videos"],
            "actionable_ratio": round(analyzable_count / len(rows), 3) if rows else 0.0,
            "ranking_top_1_return_pct": validation.get("top_1", {}).get("portfolio_return_pct", 0.0),
            "ranking_top_3_return_pct": validation.get("top_3", {}).get("portfolio_return_pct", 0.0),
            "ranking_spearman": ranking_spearman(ranking, validation),
            "ranking_eval_positions": len(validation.get(f"top_{len(ranking)}", {}).get("positions", [])),
            "quality_scorecard": payload["scorecard"],
            "skipped_videos": channel_run["skipped_videos"],
            "transcript_backed_videos": channel_run["transcript_backed_videos"],
            "metadata_fallback_videos": channel_run["metadata_fallback_videos"],
            "latest_published_at": channel_run["latest_published_at"],
            "signal_breakdown": channel_run["signal_breakdown"],
            "top_skip_reasons": channel_run["top_skip_reasons"],
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
    comparison["pipeline_summary"] = {
        "total_channels": len(channel_payloads),
        "total_videos": total_videos,
        "actionable_videos": analyzable_videos,
        "analyzable_videos": analyzable_videos,
        "strict_actionable_videos": strict_actionable_videos,
        "skipped_videos": skipped_videos,
        "transcript_backed_videos": transcript_backed_videos,
        "metadata_fallback_videos": metadata_fallback_videos,
        "latest_published_at": latest_published_at,
        "signal_breakdown": dict(aggregate_signal_breakdown),
        "top_skip_reasons": [
            {"reason": reason, "count": count}
            for reason, count in aggregate_skip_reasons.most_common(5)
        ],
    }
    return comparison
