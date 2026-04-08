from omx_brainstorm.evidence import (
    INSUFFICIENT_TRANSCRIPT_REASON,
    assess_reasoning_strength,
    build_stock_evidence,
    is_transcript_backed,
)
from omx_brainstorm.models import FundamentalSnapshot, MasterOpinion, TickerMention


def test_is_transcript_backed_rejects_metadata_fallback():
    assert is_transcript_backed("ko", "transcript_api") is True
    assert is_transcript_backed("cache:ko", "transcript_cache") is True
    assert is_transcript_backed("metadata_fallback", "metadata_fallback") is False
    assert is_transcript_backed("cache:metadata_fallback", "metadata_fallback") is False


def test_build_stock_evidence_collects_alias_sentences():
    mention = TickerMention(
        ticker="005930.KS",
        company_name="Samsung Electronics",
        evidence=["삼성전자가 HBM 공급 확대의 수혜를 본다"],
    )
    transcript = (
        "삼성전자가 HBM 공급 확대의 수혜를 본다. "
        "시장에서는 삼성전자와 SK하이닉스를 함께 본다. "
        "이번 사이클에서 Samsung Electronics 마진 회복이 중요하다."
    )

    evidence = build_stock_evidence(transcript, mention)

    assert len(evidence) >= 3
    assert any("삼성전자" in item for item in evidence)
    assert any("Samsung Electronics" in item for item in evidence)


def test_assess_reasoning_strength_uses_evidence_and_fundamentals():
    fundamentals = FundamentalSnapshot(
        ticker="NVDA",
        company_name="NVIDIA",
        current_price=100.0,
        forward_pe=25.0,
        revenue_growth=0.25,
        operating_margin=0.32,
        return_on_equity=0.28,
        debt_to_equity=40.0,
        data_source="dummy",
    )
    opinions = [
        MasterOpinion(master="druckenmiller", verdict="BUY", score=80, max_score=100, one_liner="좋다"),
        MasterOpinion(master="buffett", verdict="WATCH", score=70, max_score=100, one_liner="비싸다"),
        MasterOpinion(master="soros", verdict="BUY", score=78, max_score=100, one_liner="추세가 강하다"),
    ]

    label, summary = assess_reasoning_strength(
        evidence_bullets=[
            "엔비디아가 아직 더 갈 수 있다",
            "데이터센터 수요가 강하다",
            "AI 인프라 CAPEX가 계속 늘어난다",
        ],
        fundamentals=fundamentals,
        master_opinions=opinions,
        transcript_backed=True,
    )

    assert label == "STRONG"
    assert "실자막" in summary
    assert "재무지표" in summary


def test_insufficient_transcript_reason_mentions_transcript():
    assert "실자막" in INSUFFICIENT_TRANSCRIPT_REASON
