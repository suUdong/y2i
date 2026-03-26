"""Tests for 30-day heuristic dashboard generation and CLI subcommand."""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from omx_brainstorm.heuristic_pipeline import render_heuristic_dashboard


def _make_row(video_id="v1", title="Test Video", signal_score=70, video_type="STOCK_PICK"):
    return {
        "video_id": video_id,
        "title": title,
        "url": f"https://youtube.com/watch?v={video_id}",
        "published_at": "2026-03-01",
        "description": "",
        "tags": [],
        "video_type": video_type,
        "signal_score": signal_score,
        "video_signal_class": "ACTIONABLE",
        "should_analyze_stocks": True,
        "reason": "test",
        "skip_reason": "",
        "signal_metrics": {},
        "transcript_language": "ko",
        "macro_insights": [],
        "market_review": None,
        "expert_insights": [],
        "stocks": [],
    }


def test_render_heuristic_dashboard_creates_file(tmp_path):
    rows = [_make_row("v1", "Video A"), _make_row("v2", "Video B")]
    result = render_heuristic_dashboard(rows, tmp_path, label="test_dashboard")
    assert result is not None
    assert result.exists()
    content = result.read_text(encoding="utf-8")
    assert "OMX 통합 대시보드" in content
    assert "분석 영상 목록" in content


def test_render_heuristic_dashboard_empty_rows(tmp_path):
    result = render_heuristic_dashboard([], tmp_path, label="empty_dash")
    assert result is None


def test_render_heuristic_dashboard_with_stocks(tmp_path):
    row = _make_row("v1", "반도체 종목 분석")
    row["stocks"] = [{
        "ticker": "005930.KS",
        "company_name": "삼성전자",
        "mention_count": 3,
        "signal_timestamp": "2026-03-01",
        "signal_strength_score": 0.8,
        "evidence_source": "transcript",
        "evidence_snippets": ["반도체"],
        "basic_state": "양호",
        "basic_signal_summary": "매출성장률 10%",
        "basic_signal_verdict": "BUY",
        "fundamentals": {
            "ticker": "005930.KS",
            "company_name": "삼성전자",
            "data_source": "mock",
            "notes": [],
        },
        "master_opinions": [
            {
                "master": "buffett",
                "verdict": "BUY",
                "score": 75.0,
                "max_score": 100.0,
                "one_liner": "견고한 이익 구조",
                "rationale": [],
                "risks": [],
                "citations": [],
            }
        ],
        "final_score": 72.0,
        "final_verdict": "BUY",
        "invalidation_triggers": ["실적 둔화"],
    }]
    result = render_heuristic_dashboard([row], tmp_path, label="stock_dash")
    assert result is not None
    content = result.read_text(encoding="utf-8")
    assert "삼성전자" in content
    assert "005930.KS" in content


def test_render_heuristic_dashboard_with_expert_insights(tmp_path):
    row = _make_row("v1", "전문가 인터뷰", video_type="EXPERT_INTERVIEW")
    row["expert_insights"] = [{
        "expert_name": "김철수",
        "affiliation": "한국증권",
        "key_claims": ["반도체 상승 전망"],
        "topic": "반도체",
        "sentiment": "BULLISH",
        "mentioned_tickers": ["005930.KS"],
    }]
    result = render_heuristic_dashboard([row], tmp_path, label="expert_dash")
    assert result is not None
    content = result.read_text(encoding="utf-8")
    assert "전문가 인사이트" in content
    assert "김철수" in content


def test_render_heuristic_dashboard_with_macro(tmp_path):
    row = _make_row("v1", "시장 리뷰", video_type="MARKET_REVIEW")
    row["macro_insights"] = [{
        "indicator": "interest_rate",
        "direction": "DOWN",
        "label": "기준금리",
        "confidence": 0.85,
        "matched_keywords": ["금리인하"],
        "sentiment": "BULLISH",
        "beneficiary_sectors": ["부동산", "건설"],
    }]
    result = render_heuristic_dashboard([row], tmp_path, label="macro_dash")
    assert result is not None
    content = result.read_text(encoding="utf-8")
    assert "매크로 시그널 요약" in content
    assert "기준금리" in content


def test_cli_analyze_channel_30d_subcommand():
    """Verify the CLI parser accepts analyze-channel-30d subcommand."""
    from omx_brainstorm.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["analyze-channel-30d", "sampro", "--days", "7"])
    assert args.command == "analyze-channel-30d"
    assert args.slug == "sampro"
    assert args.days == 7


def test_cli_analyze_channel_30d_default_days():
    from omx_brainstorm.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(["analyze-channel-30d", "itgod"])
    assert args.days == 30
