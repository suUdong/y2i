from omx_brainstorm.macro_signals import extract_macro_signals, indirect_macro_mentions
from omx_brainstorm.signal_gate import assess_video_signal


def test_extract_macro_signals_detects_rate_cut_theme():
    signals = extract_macro_signals("금리 인하가 시작되면 성장주와 부동산, 건설주가 반등할 수 있다")
    names = {item["name"] for item in signals}
    assert "rate_cut" in names
    rate_cut = next(item for item in signals if item["name"] == "rate_cut")
    assert "growth_tech" in rate_cut["beneficiary_sectors"]
    assert "construction" in rate_cut["beneficiary_sectors"]


def test_indirect_macro_mentions_returns_representative_stocks():
    mentions = indirect_macro_mentions(
        video_title="금리 인하가 오면 부동산과 건설주 어디까지 오를까",
        transcript_text="금리 인하와 원화 강세가 겹치면 건설, 증권, 성장주가 좋아질 수 있다.",
    )
    tickers = {item.ticker for item in mentions}
    assert "000720.KS" in tickers
    assert "006800.KS" in tickers
    assert "035420.KS" in tickers


def test_signal_gate_promotes_macro_sector_content_to_actionable():
    result = assess_video_signal(
        title="금리 인하가 오면 건설주와 성장주가 어떻게 움직일까",
        transcript_text="금리 인하, 원화 강세, 경기 회복 국면에서 수혜 섹터를 정리한다.",
        description="부동산, 건설, 증권, 성장주를 함께 다룬다.",
        tags=["금리인하", "부동산", "건설주", "성장주", "투자"],
    )

    assert result.video_signal_class == "ACTIONABLE"
    assert result.should_analyze_stocks is True
    assert result.metrics["macro_signal_count"] >= 1


def test_signal_gate_does_not_promote_generic_risk_off_to_actionable():
    result = assess_video_signal(
        title="주식시장 정말 위험 신호 터졌다, 지금은 현금이 답인가",
        transcript_text="폭락 가능성과 리스크 관리 이야기를 한다.",
        description="현금 비중과 조심스러운 대응을 강조한다.",
        tags=["폭락", "현금", "조심"],
    )

    assert result.video_signal_class != "ACTIONABLE"


def test_signal_gate_generic_title_not_rescued_by_tags_only():
    result = assess_video_signal(
        title="폭락 후 부자되는 사람들의 특징",
        transcript_text="",
        description="존리 대표 인터뷰",
        tags=["삼성전자", "반도체", "SK하이닉스", "주식"],
    )

    assert result.video_signal_class != "ACTIONABLE"
