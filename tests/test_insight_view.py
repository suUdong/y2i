from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.insight_view import (
    build_channel_focus_payload,
    build_feed_items,
    build_header_metrics,
    build_priority_signal_rows,
    build_ranking_items,
    build_recent_video_rows,
    build_stock_insight_cards,
)


def test_build_header_metrics_uses_comparison_and_live_feed():
    metrics = build_header_metrics(
        overview={"channel_count": 2, "analyzable_count": 5},
        comparison={
            "pipeline_summary": {"total_channels": 3, "actionable_videos": 7},
            "signal_accuracy": {"overall": {"total_signals": 12}},
            "consensus_signals": [{"ticker": "NVDA"}],
        },
        live_feed={"last_update": "2026-04-08T03:20:00+00:00"},
    )
    assert metrics[0]["value"] == "2026-04-08T03:20:00+00:00"
    assert metrics[1]["value"] == "3"
    assert metrics[2]["value"] == "7"
    assert metrics[3]["value"] == "12"
    assert metrics[4]["value"] == "1"


def test_build_priority_signal_rows_prefers_consensus_and_summary():
    rows = build_priority_signal_rows(
        [
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "aggregate_score": 88.5,
                "aggregate_verdict": "BUY",
                "consensus_signal": True,
                "signal_kind": "CONSENSUS",
                "_source_channels_display": ["삼프로TV", "이형수"],
                "source_video_summaries": [
                    {"video_context_summary": "데이터센터 수요와 AI 인프라 CAPEX를 핵심 논거로 제시"},
                ],
            }
        ]
    )
    assert rows[0]["ticker"].startswith("NVDA")
    assert rows[0]["signal_type"] == "CONSENSUS"
    assert "데이터센터" in rows[0]["why_now"]


def test_build_recent_video_rows_uses_video_summary_or_reason():
    rows = build_recent_video_rows(
        [
            {
                "_channel": "sampro",
                "title": "반도체 전망",
                "video_signal_class": "ACTIONABLE",
                "signal_score": 71.0,
                "video_type": "STOCK_PICK",
                "video_summary": "반도체 업황 회복과 HBM 수요를 강조",
            },
            {
                "_channel": "itgod",
                "title": "매크로 잡담",
                "video_signal_class": "NOISE",
                "signal_score": 12.0,
                "video_type": "OTHER",
                "skip_reason": "근거 부족",
            },
        ],
        {"sampro": "삼프로TV", "itgod": "IT의 신"},
    )
    assert rows[0]["channel"] == "삼프로TV"
    assert "반도체 업황" in rows[0]["why"]
    assert rows[1]["why"] == "근거 부족"


def test_build_channel_focus_payload_extracts_stocks_macro_and_experts():
    payload = build_channel_focus_payload(
        "sampro",
        {
            "channel_name": "삼프로TV",
            "videos": [
                {
                    "title": "반도체 인터뷰",
                    "video_signal_class": "ACTIONABLE",
                    "signal_score": 75.0,
                    "video_summary": "HBM 수요 회복을 언급",
                    "stocks": [{"ticker": "005930.KS"}],
                    "macro_insights": [
                        {"indicator": "interest_rate", "direction": "DOWN", "label": "금리", "confidence": 0.8}
                    ],
                    "expert_insights": [
                        {
                            "expert_name": "김철수",
                            "topic": "반도체",
                            "summary": "하반기 메모리 업황 회복",
                            "key_claims": [],
                            "mentioned_tickers": ["005930.KS"],
                            "structured_claims": [{"claim": "HBM 수요 회복", "direction": "BULLISH", "confidence": 0.82}],
                        }
                    ],
                }
            ],
            "cross_video_ranking": [
                {
                    "ticker": "005930.KS",
                    "company_name": "Samsung Electronics",
                    "aggregate_score": 80.0,
                    "aggregate_verdict": "BUY",
                    "source_video_summaries": [
                        {
                            "video_title": "반도체 인터뷰",
                            "thesis_summary": "HBM 수요 회복",
                            "video_context_summary": "메모리 업황이 돌아서고 있다고 평가",
                            "plain_summary": "AI 서버 수요가 반도체 투자심리를 지지한다.",
                            "signal_summary": "성장과 수요 회복",
                            "final_score": 81.0,
                            "verdict": "BUY",
                            "master_opinions": [
                                {"master": "druckenmiller", "verdict": "BUY", "score": 84.0, "one_liner": "수요 드라이버가 선명하다."},
                                {"master": "buffett", "verdict": "WATCH", "score": 68.0, "one_liner": "사업 질은 높지만 가격은 봐야 한다."},
                            ],
                            "fundamentals": {
                                "company_name": "Samsung Electronics",
                                "currency": "KRW",
                                "current_price": 80000,
                                "forward_pe": 14.2,
                                "revenue_growth": 0.12,
                                "operating_margin": 0.19,
                                "return_on_equity": 0.11,
                                "debt_to_equity": 28.0,
                                "recommendation_key": "buy",
                                "analyst_count": 18,
                                "analyst_buy": 11,
                                "analyst_hold": 7,
                                "target_median_price": 92000,
                            },
                        }
                    ],
                    "price_target": {"target_price": 92000, "currency": "KRW"},
                }
            ],
        },
    )
    assert payload["channel_name"] == "삼프로TV"
    assert payload["top_stocks"][0]["verdict"] == "BUY"
    assert payload["macro_points"][0]["label"] == "금리"
    assert payload["expert_points"][0]["expert"] == "김철수"
    assert payload["stock_cards"][0]["expert_views"][0]["expert"] == "김철수"
    assert payload["stock_cards"][0]["master_views"][0]["master"] == "druckenmiller"
    assert payload["stock_cards"][0]["channel_count"] == 1
    assert "매수 우위" in payload["stock_cards"][0]["analyst_summary"]


def test_build_stock_insight_cards_merges_experts_masters_and_fundamentals():
    cards = build_stock_insight_cards(
        [
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "aggregate_score": 88.5,
                "aggregate_verdict": "BUY",
                "signal_kind": "CONSENSUS",
                "consensus_signal": True,
                "consensus_strength": "STRONG",
                "channel_count": 2,
                "_source_channels_display": ["삼프로TV", "IT의 신"],
                "source_video_summaries": [
                    {
                        "video_title": "엔비디아 데이터센터 투자",
                        "video_context_summary": "데이터센터 수요와 AI 인프라 투자를 근거로 제시",
                        "plain_summary": "좋은 회사지만 기대치가 높다.",
                        "signal_summary": "AI 인프라 수요",
                        "thesis_summary": "CAPEX 확대",
                        "final_score": 87.0,
                        "verdict": "BUY",
                        "master_opinions": [
                            {"master": "druckenmiller", "verdict": "BUY", "score": 83.0, "one_liner": "성장 드라이버가 강하다."},
                            {"master": "buffett", "verdict": "WATCH", "score": 69.0, "one_liner": "가격 부담은 점검해야 한다."},
                            {"master": "soros", "verdict": "BUY", "score": 78.0, "one_liner": "내러티브가 아직 강하다."},
                        ],
                        "fundamentals": {
                            "company_name": "NVIDIA",
                            "currency": "USD",
                            "current_price": 910.0,
                            "forward_pe": 34.5,
                            "revenue_growth": 0.41,
                            "operating_margin": 0.37,
                            "return_on_equity": 0.46,
                            "debt_to_equity": 32.0,
                            "recommendation_key": "buy",
                            "analyst_count": 42,
                            "analyst_strong_buy": 12,
                            "analyst_buy": 21,
                            "analyst_hold": 7,
                            "target_median_price": 1000.0,
                        },
                    }
                ],
                "price_target": {"target_price": 1000.0, "currency": "USD"},
            }
        ],
        videos=[
            {
                "title": "엔비디아 데이터센터 투자",
                "stocks": [{"ticker": "NVDA"}],
                "expert_insights": [
                    {
                        "expert_name": "김철수",
                        "topic": "AI 반도체",
                        "summary": "AI 서버 투자와 데이터센터 CAPEX가 이어질 것",
                        "sentiment": "BULLISH",
                        "mentioned_tickers": ["NVDA"],
                        "structured_claims": [
                            {"claim": "데이터센터 수요가 강하다", "direction": "BULLISH", "confidence": 0.85}
                        ],
                    }
                ],
            }
        ],
    )
    assert cards[0]["ticker_display"].startswith("NVDA")
    assert cards[0]["conviction"] == "HIGH"
    assert cards[0]["expert_views"][0]["expert"] == "김철수"
    assert cards[0]["master_views"][0]["master"] == "druckenmiller"
    assert cards[0]["fundamental_rows"][0]["label"] == "현재가"
    assert "애널리스트 42명" in cards[0]["analyst_summary"]
    assert "$1,000.00" in cards[0]["price_target_summary"]


def test_build_feed_and_ranking_items_surface_scan_data():
    cards = [
        {
            "ticker": "NVDA",
            "ticker_display": "NVDA NVIDIA",
            "score": 88.5,
            "verdict": "BUY",
            "why_now": "AI 인프라와 데이터센터 수요",
            "plain_summary": "좋은 회사지만 기대치가 높다.",
            "video_context_summary": "데이터센터 수요를 근거로 제시",
            "source_channels": ["삼프로TV", "IT의 신"],
            "expert_views": [{"expert": "김철수", "topic": "AI 반도체"}],
            "signal_note": "2채널 STRONG",
            "last_signal_at": "2026-04-08",
            "latest_checked_at": "2026-04-08T00:00:00+00:00",
            "conviction": "HIGH",
            "channel_count": 2,
        },
        {
            "ticker": "005930.KS",
            "ticker_display": "005930 삼성전자",
            "score": 75.0,
            "verdict": "WATCH",
            "why_now": "HBM 회복 기대",
            "plain_summary": "",
            "video_context_summary": "",
            "source_channels": ["삼프로TV"],
            "expert_views": [],
            "signal_note": "SINGLE SOURCE",
            "last_signal_at": "2026-04-07",
            "latest_checked_at": "2026-04-08T00:00:00+00:00",
            "conviction": "MEDIUM",
            "channel_count": 1,
        },
    ]
    feed_items = build_feed_items(cards)
    ranking_items = build_ranking_items(cards)

    assert feed_items[0]["ticker"] == "NVDA"
    assert feed_items[0]["source"] == "김철수 · AI 반도체"
    assert ranking_items[0]["ticker"] == "NVDA"
    assert ranking_items[0]["channels"] == "삼프로TV, IT의 신"
