from __future__ import annotations

from .models import FundamentalSnapshot


def _pct(value: float | None) -> float | None:
    """Convert ratio (0.30) to percentage (30.0). Returns None if input is None."""
    return None if value is None else value * 100.0


def _describe_margin(pct: float) -> str:
    if pct >= 20:
        return f"돈을 아주 잘 버는 회사 (영업이익률 {pct:.1f}%)"
    if pct >= 10:
        return f"돈을 꽤 잘 버는 회사 (영업이익률 {pct:.1f}%)"
    if pct >= 5:
        return f"돈을 보통 수준으로 버는 회사 (영업이익률 {pct:.1f}%)"
    return f"돈 벌기가 어려운 회사 (영업이익률 {pct:.1f}%)"


def _describe_roe(pct: float) -> str:
    if pct >= 15:
        return f"투자한 돈 대비 수익이 좋음 (ROE {pct:.1f}%)"
    if pct >= 8:
        return f"투자 대비 수익이 보통 (ROE {pct:.1f}%)"
    return f"투자 대비 수익이 낮음 (ROE {pct:.1f}%)"


def _describe_debt(de: float) -> str:
    if de < 50:
        return f"빚이 아주 적음 (부채비율 {de:.1f}%)"
    if de < 100:
        return f"빚은 적당한 편 (부채비율 {de:.1f}%)"
    if de < 200:
        return f"빚이 좀 있는 편 (부채비율 {de:.1f}%)"
    return f"빚이 많은 편 (부채비율 {de:.1f}%)"


def _describe_pe(pe: float) -> str:
    if pe < 15:
        return f"주가가 저렴한 편 (PER {pe:.1f}배)"
    if pe < 25:
        return f"주가가 적당한 편 (PER {pe:.1f}배)"
    if pe < 40:
        return f"주가가 비싼 편 (PER {pe:.1f}배)"
    return f"주가가 아주 비쌈 (PER {pe:.1f}배)"


def _describe_revenue_growth(pct: float) -> str:
    if pct >= 30:
        return f"매출이 빠르게 성장 중 (매출성장률 {pct:.1f}%)"
    if pct >= 10:
        return f"매출이 꾸준히 성장 중 (매출성장률 {pct:.1f}%)"
    if pct >= 0:
        return f"매출이 조금씩 성장 중 (매출성장률 {pct:.1f}%)"
    return f"매출이 줄어들고 있음 (매출성장률 {pct:.1f}%)"


def _format_price(price: float, currency: str | None) -> str:
    if currency == "KRW":
        return f"\\{price:,.0f}"
    if currency == "USD":
        return f"${price:,.2f}"
    return f"{price:,.0f}"


def build_plain_summary(snapshot: FundamentalSnapshot) -> str:
    """Convert financial metrics to plain Korean summary lines."""
    lines: list[str] = []
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    de = snapshot.debt_to_equity
    pe = snapshot.forward_pe
    rev = _pct(snapshot.revenue_growth)

    if margin is not None:
        lines.append(_describe_margin(margin))
    if roe is not None:
        lines.append(_describe_roe(roe))
    if de is not None:
        lines.append(_describe_debt(de))
    if pe is not None and pe > 0:
        lines.append(_describe_pe(pe))
    if rev is not None:
        lines.append(_describe_revenue_growth(rev))

    if not lines:
        return "데이터 부족"

    analyst = build_analyst_summary(snapshot)
    if analyst and "없어요" not in analyst:
        lines.append(analyst)
    return "\n".join(lines)


def build_analyst_summary(snapshot: FundamentalSnapshot) -> str:
    """Convert analyst consensus to plain Korean."""
    count = snapshot.analyst_count
    if not count or count == 0:
        return "애널리스트 의견이 없어요"

    sb = snapshot.analyst_strong_buy
    b = snapshot.analyst_buy
    h = snapshot.analyst_hold
    s = snapshot.analyst_sell
    ss = snapshot.analyst_strong_sell
    total_votes = sb + b + h + s + ss

    if total_votes == 0:
        return "애널리스트 의견이 없어요"

    buy_count = sb + b
    sell_count = s + ss
    buy_pct = buy_count / total_votes * 100
    hold_pct = h / total_votes * 100
    sell_pct = sell_count / total_votes * 100

    target = snapshot.target_median_price or snapshot.target_mean_price
    target_str = ""
    if target:
        target_str = f" (목표가 {_format_price(target, snapshot.currency)})"

    if sell_pct >= 30:
        return f"주의: {count}명 전문가 중 {sell_count}명이 '팔아라' 의견{target_str}"
    if hold_pct >= 50:
        return f"{count}명 전문가들이 '지켜보자'는 의견{target_str}"
    if buy_pct >= 70:
        return f"{count}명 전문가 중 {buy_count}명이 '사라'고 함{target_str}"
    if buy_pct >= 50:
        return f"{count}명 전문가 절반 이상이 '사라'고 함{target_str}"
    return f"{count}명 전문가 의견 혼재 (매수 {buy_count}, 보유 {h}, 매도 {sell_count}){target_str}"
