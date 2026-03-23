import pytest

from omx_brainstorm.master_engine import validate_master_opinions
from omx_brainstorm.models import MasterOpinion


def test_validate_master_opinions_accepts_valid_items():
    opinions = [
        MasterOpinion("druckenmiller", "BUY", 80, 100, "A", ["r"], ["x"], ["fundamentals:a", "evidence:b"]),
        MasterOpinion("buffett", "WATCH", 70, 100, "B", ["r"], ["x"], ["fundamentals:a", "evidence:b"]),
        MasterOpinion("soros", "BUY", 75, 100, "C", ["r"], ["x"], ["fundamentals:a", "evidence:b"]),
    ]
    assert validate_master_opinions(opinions) == opinions


def test_validate_master_opinions_rejects_duplicate_lines():
    opinions = [
        MasterOpinion("druckenmiller", "BUY", 80, 100, "A", ["r"], ["x"], ["fundamentals:a", "evidence:b"]),
        MasterOpinion("buffett", "WATCH", 70, 100, "A", ["r"], ["x"], ["fundamentals:a", "evidence:b"]),
    ]
    with pytest.raises(ValueError):
        validate_master_opinions(opinions)


def test_validate_master_opinions_requires_fundamentals_citation():
    with pytest.raises(ValueError):
        validate_master_opinions([MasterOpinion("druckenmiller", "BUY", 80, 100, "A", ["r"], ["x"], ["evidence:b"])])


def test_validate_master_opinions_requires_evidence_citation():
    with pytest.raises(ValueError):
        validate_master_opinions([MasterOpinion("druckenmiller", "BUY", 80, 100, "A", ["r"], ["x"], ["fundamentals:a"])])
