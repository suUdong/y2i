import pytest
from pathlib import Path

from omx_brainstorm.master_engine import (
    MasterConfig,
    build_master_opinions,
    build_opinion,
    format_template,
    load_masters,
    master_variance_score,
    master_verdict,
    validate_cross_stock_master_quality,
    validate_master_opinions,
)
from omx_brainstorm.models import FundamentalSnapshot


def test_load_masters_from_toml():
    masters = load_masters()
    assert len(masters) >= 3
    names = [m.name for m in masters]
    assert "druckenmiller" in names
    assert "buffett" in names
    assert "soros" in names


def test_master_config_has_required_fields():
    masters = load_masters()
    for m in masters:
        assert m.name
        assert m.display
        assert m.focus
        assert m.base_score > 0
        assert isinstance(m.weights, dict)
        assert isinstance(m.risks, list)
        assert isinstance(m.templates, dict)
        assert "one_liner" in m.templates
        assert "rationale" in m.templates


def test_build_opinion_produces_valid_opinion():
    masters = load_masters()
    config = masters[0]  # druckenmiller
    metrics = {
        "ticker": "NVDA",
        "evidence": "AI 수요 급증",
        "mention_count": 5,
        "video_signal_score": 90.0,
        "revenue": 73.2,
        "margin": 65.0,
        "roe": 101.4,
        "momentum": 42.2,
        "forward_pe": 15.5,
        "leverage": 7.2,
        "theme_score": 1,
    }
    opinion = build_opinion(config, metrics)
    assert opinion.master == "druckenmiller"
    assert 0 <= opinion.score <= 100
    assert opinion.verdict in ("BUY", "WATCH", "REJECT")
    assert opinion.one_liner
    assert opinion.rationale
    assert any(c.startswith("fundamentals:") for c in opinion.citations)
    assert any(c.startswith("evidence:") for c in opinion.citations)


def test_build_master_opinions_backward_compatible():
    """build_master_opinions() should still work with old signature."""
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
        evidence_snippets=["엔비디아가 제시한 차세대 메모리 로드맵"],
    )
    assert len(opinions) >= 3
    assert len({item.one_liner for item in opinions}) == len(opinions)
    for item in opinions:
        assert item.rationale
        assert any(c.startswith("fundamentals:") for c in item.citations)
        assert any(c.startswith("evidence:") for c in item.citations)


def test_format_template():
    template = "{ticker}는 매출 {revenue:.1f}%"
    result = format_template(template, {"ticker": "AAPL", "revenue": 25.3})
    assert result == "AAPL는 매출 25.3%"


def test_format_template_missing_key():
    template = "{ticker}는 {missing_key}를 보유"
    result = format_template(template, {"ticker": "AAPL"})
    assert "AAPL" in result  # should not crash


def test_cross_stock_master_quality_guard_rejects_repeated_sentences():
    stocks = [
        {
            "ticker": "AAA",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
        {
            "ticker": "BBB",
            "master_opinions": [
                {"master": "druckenmiller", "one_liner": "같은 문장", "citations": ["fundamentals:x", "evidence:y"]},
            ],
        },
    ]
    with pytest.raises(ValueError):
        validate_cross_stock_master_quality(stocks)
