from omx_brainstorm.models import FundamentalSnapshot
from omx_brainstorm.plain_summary import build_plain_summary, build_analyst_summary


def test_build_plain_summary_high_margin():
    snap = FundamentalSnapshot(
        ticker="TEST",
        operating_margin=0.30,
        return_on_equity=0.18,
        debt_to_equity=45.0,
        forward_pe=20.0,
        revenue_growth=0.25,
    )
    result = build_plain_summary(snap)
    assert "돈을 아주 잘 버는 회사" in result
    assert "영업이익률 30.0%" in result
    assert "투자한 돈 대비 수익이 좋음" in result
    assert "빚이 아주 적음" in result
    assert "주가가 적당한 편" in result
    assert "매출이 꾸준히 성장 중" in result


def test_build_plain_summary_low_margin():
    snap = FundamentalSnapshot(
        ticker="TEST",
        operating_margin=0.03,
        return_on_equity=0.05,
        debt_to_equity=250.0,
        forward_pe=50.0,
        revenue_growth=-0.1,
    )
    result = build_plain_summary(snap)
    assert "돈 벌기가 어려운 회사" in result
    assert "투자 대비 수익이 낮음" in result
    assert "빚이 많은 편" in result
    assert "주가가 아주 비쌈" in result
    assert "매출이 줄어들고 있음" in result


def test_build_plain_summary_none_metrics():
    snap = FundamentalSnapshot(ticker="TEST")
    result = build_plain_summary(snap)
    assert isinstance(result, str)
    assert "데이터 부족" in result or result == ""


def test_build_analyst_summary_bullish():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=37,
        analyst_strong_buy=12,
        analyst_buy=23,
        analyst_hold=1,
        analyst_sell=0,
        analyst_strong_sell=0,
        target_median_price=250000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "37명" in result
    assert "35명" in result  # 12+23
    assert "사라" in result
    assert "250,000" in result


def test_build_analyst_summary_no_data():
    snap = FundamentalSnapshot(ticker="TEST")
    result = build_analyst_summary(snap)
    assert "애널리스트 의견이 없어요" in result


def test_build_analyst_summary_mixed():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=20,
        analyst_strong_buy=2,
        analyst_buy=6,
        analyst_hold=10,
        analyst_sell=1,
        analyst_strong_sell=1,
        target_median_price=50000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "지켜보자" in result


def test_build_analyst_summary_sell_warning():
    snap = FundamentalSnapshot(
        ticker="TEST",
        analyst_count=10,
        analyst_strong_buy=0,
        analyst_buy=2,
        analyst_hold=4,
        analyst_sell=3,
        analyst_strong_sell=1,
        target_median_price=30000.0,
        currency="KRW",
    )
    result = build_analyst_summary(snap)
    assert "팔아라" in result
