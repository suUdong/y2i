import pytest

from omx_brainstorm.master_engine import build_master_opinions, validate_cross_stock_master_quality
from omx_brainstorm.models import FundamentalSnapshot


def test_build_master_opinions_are_stock_specific_and_cited():
    snapshot = FundamentalSnapshot(
        ticker="NVDA",
        company_name="NVIDIA Corporation",
        current_price=172.7,
        forward_pe=15.5,
        revenue_growth=0.732,
        operating_margin=0.6502,
        return_on_equity=1.014,
        debt_to_equity=7.2,
        fifty_two_week_change=0.422,
        currency="USD",
    )
    opinions = build_master_opinions(
        ticker="NVDA",
        company_name="NVIDIA Corporation",
        snapshot=snapshot,
        mention_count=5,
        video_title="엔비디아 차세대 메모리 로드맵",
        video_signal_score=90.0,
        evidence_snippets=["엔비디아가 제시한 차세대 메모리 로드맵", "데이터센터 수요가 강하다"],
    )

    assert len(opinions) == 3
    assert len({item.one_liner for item in opinions}) == 3
    for item in opinions:
        assert item.rationale
        assert any(c.startswith("fundamentals:") for c in item.citations)
        assert any(c.startswith("evidence:") for c in item.citations)


def test_cross_stock_master_quality_guard_rejects_repeated_sentences():
    stocks = [
        {
            "ticker": "AAA",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
                {"master": "buffett", "one_liner": "버핏 문장 A", "citations": ["fundamentals:x", "evidence:y"]},
                {"master": "soros", "one_liner": "소로스 문장 A", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
        {
            "ticker": "BBB",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
                {"master": "buffett", "one_liner": "버핏 문장 B", "citations": ["fundamentals:x", "evidence:y"]},
                {"master": "soros", "one_liner": "소로스 문장 B", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
    ]

    with pytest.raises(ValueError):
        validate_cross_stock_master_quality(stocks)
