"""Tests for LLM-based expert claim extraction."""
import json
from unittest.mock import MagicMock

from omx_brainstorm.expert_interview import (
    extract_expert_claims_llm,
    extract_expert_insights,
    extract_expert_insights_with_llm,
)
from omx_brainstorm.llm import MockProvider
from omx_brainstorm.models import StructuredClaim


SAMPLE_TITLE = "긴급! 반도체 대전망 | 김철수 삼성증권 애널리스트"
SAMPLE_TEXT = (
    "반도체 업황이 하반기부터 본격 회복될 것으로 전망합니다. "
    "메모리 재고 조정이 마무리 국면이고 AI 서버 수요가 견인하고 있습니다. "
    "금리 인하 시기가 예상보다 늦어질 수 있어 주의가 필요합니다. "
    "삼성전자와 SK하이닉스가 수혜를 받을 것으로 판단됩니다."
)


def test_extract_expert_claims_llm_with_mock():
    provider = MockProvider()
    claims = extract_expert_claims_llm(provider, SAMPLE_TITLE, SAMPLE_TEXT)
    assert len(claims) == 2
    assert isinstance(claims[0], StructuredClaim)
    assert claims[0].direction == "BULLISH"
    assert claims[1].direction == "BEARISH"
    assert claims[0].confidence == 0.85


def test_extract_expert_claims_llm_returns_max_5():
    payload = {"claims": [
        {"claim": f"claim {i}", "reasoning": "r", "confidence": 0.5, "direction": "NEUTRAL"}
        for i in range(8)
    ]}
    provider = MagicMock()
    provider.run_json.return_value = payload
    claims = extract_expert_claims_llm(provider, "title", "text")
    assert len(claims) == 5


def test_extract_expert_claims_llm_fallback_on_error():
    provider = MagicMock()
    provider.run_json.side_effect = RuntimeError("LLM unavailable")
    claims = extract_expert_claims_llm(provider, "title", "text")
    assert claims == []


def test_extract_expert_claims_llm_empty_claims():
    provider = MagicMock()
    provider.run_json.return_value = {"claims": []}
    claims = extract_expert_claims_llm(provider, "title", "text")
    assert claims == []


def test_extract_expert_claims_llm_missing_fields():
    provider = MagicMock()
    provider.run_json.return_value = {"claims": [{"claim": "test only"}]}
    claims = extract_expert_claims_llm(provider, "title", "text")
    assert len(claims) == 1
    assert claims[0].claim == "test only"
    assert claims[0].reasoning == ""
    assert claims[0].confidence == 0.5
    assert claims[0].direction == "NEUTRAL"


def test_extract_expert_insights_with_llm_success():
    provider = MockProvider()
    insights = extract_expert_insights_with_llm(
        provider, SAMPLE_TITLE, SAMPLE_TEXT, ""
    )
    assert len(insights) >= 1
    # Should have structured claims from LLM
    assert len(insights[0].structured_claims) == 2
    assert insights[0].structured_claims[0].direction == "BULLISH"


def test_extract_expert_insights_with_llm_fallback_to_heuristic():
    """When LLM fails, structured_claims should be populated from heuristic claims."""
    provider = MagicMock()
    provider.run_json.side_effect = RuntimeError("LLM down")
    insights = extract_expert_insights_with_llm(
        provider, SAMPLE_TITLE, SAMPLE_TEXT, ""
    )
    assert len(insights) >= 1
    # Fallback: structured claims from heuristic key_claims
    for insight in insights:
        assert len(insight.structured_claims) > 0
        for sc in insight.structured_claims:
            assert sc.claim in insight.key_claims


def test_extract_expert_insights_with_llm_no_experts():
    """When no expert is detected, return empty list."""
    provider = MockProvider()
    insights = extract_expert_insights_with_llm(
        provider, "일반 뉴스", "오늘 증시 동향입니다", ""
    )
    assert insights == []


def test_structured_claim_dataclass():
    claim = StructuredClaim(
        claim="반도체 상승",
        reasoning="수요 증가",
        confidence=0.9,
        direction="BULLISH",
    )
    assert claim.claim == "반도체 상승"
    assert claim.confidence == 0.9


def test_expert_insight_structured_claims_default():
    """ExpertInsight should have empty structured_claims by default."""
    from omx_brainstorm.models import ExpertInsight
    insight = ExpertInsight(
        expert_name="테스트",
        affiliation="테스트증권",
        key_claims=["claim1"],
    )
    assert insight.structured_claims == []


def test_expert_insight_to_dict_includes_structured_claims():
    """Verify structured_claims survives asdict serialization."""
    from dataclasses import asdict
    from omx_brainstorm.models import ExpertInsight
    insight = ExpertInsight(
        expert_name="김철수",
        affiliation="삼성증권",
        key_claims=["claim1"],
        structured_claims=[
            StructuredClaim(claim="test", reasoning="r", confidence=0.8, direction="BULLISH")
        ],
    )
    d = asdict(insight)
    assert len(d["structured_claims"]) == 1
    assert d["structured_claims"][0]["claim"] == "test"
    assert d["structured_claims"][0]["direction"] == "BULLISH"
