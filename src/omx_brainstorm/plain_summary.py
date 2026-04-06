from __future__ import annotations

from .models import FundamentalSnapshot


def _pct(value: float | None) -> float | None:
    """Convert ratio (0.30) to percentage (30.0). Returns None if input is None."""
    return None if value is None else value * 100.0


def _format_price(price: float, currency: str | None) -> str:
    if currency == "KRW":
        return f"\\{price:,.0f}"
    if currency == "USD":
        return f"${price:,.2f}"
    return f"{price:,.0f}"


def _margin_phrase(pct: float) -> str:
    if pct >= 20:
        return f"매출 100원 중 {pct:.0f}원이 남을 정도로 마진이 좋고"
    if pct >= 10:
        return f"매출 대비 {pct:.0f}% 정도 남기니 수익성은 괜찮은 편이고"
    if pct >= 5:
        return f"영업이익률 {pct:.0f}%로 남는 게 많진 않고"
    return f"영업이익률 {pct:.0f}%라 사실상 남는 게 거의 없는 상태인데"


def _roe_phrase(pct: float) -> str:
    if pct >= 15:
        return f"투자금 대비 {pct:.0f}%를 벌어오니 효율도 좋아요"
    if pct >= 8:
        return f"투자 대비 수익률은 {pct:.0f}%로 보통이에요"
    return f"투자금 대비 {pct:.0f}%밖에 못 벌고 있어요"


def _debt_phrase(de: float) -> str:
    if de < 50:
        return f"빚은 거의 없고 (부채비율 {de:.0f}%)"
    if de < 100:
        return f"빚은 감당할 만한 수준이고 (부채비율 {de:.0f}%)"
    if de < 200:
        return f"빚이 좀 있는 편이에요 (부채비율 {de:.0f}%)"
    return f"빚이 꽤 많아요 (부채비율 {de:.0f}%)"


def _pe_phrase(pe: float) -> str:
    if pe < 15:
        return f"현재 주가는 실적 대비 싼 편이에요 (PER {pe:.0f}배)"
    if pe < 25:
        return f"주가는 실적 대비 적당해요 (PER {pe:.0f}배)"
    if pe < 40:
        return f"주가가 실적 대비 비싼 편이에요 (PER {pe:.0f}배)"
    return f"주가가 실적 대비 많이 비싸요 (PER {pe:.0f}배)"


def _rev_phrase(pct: float) -> str:
    if pct >= 30:
        return f"매출은 작년보다 {pct:.0f}%나 늘었어요"
    if pct >= 10:
        return f"매출이 {pct:.0f}% 성장하면서 꾸준히 크고 있어요"
    if pct >= 0:
        return f"매출은 {pct:.0f}% 소폭 늘었어요"
    return f"매출이 작년보다 {abs(pct):.0f}% 줄었어요"


def _pick_edge(
    rev: float | None,
    margin: float | None,
    roe: float | None,
    de: float | None,
    pe: float | None,
) -> str:
    """Pick the single most notable tension or highlight as a punchy one-liner."""
    # Growth exploding but no profit
    if rev is not None and rev >= 30 and margin is not None and margin < 5:
        return f"매출 {rev:.0f}% 폭발인데 남는 게 없다"
    # Great margins but crazy expensive
    if margin is not None and margin >= 20 and pe is not None and pe >= 40:
        return f"마진 {margin:.0f}%로 잘 벌지만 PER {pe:.0f}배는 너무 비싸다"
    # Cheap + profitable = classic value
    if margin is not None and margin >= 15 and pe is not None and 0 < pe < 15:
        return f"마진도 좋고 PER {pe:.0f}배로 싸다 — 저평가 가능성"
    # Revenue declining + expensive
    if rev is not None and rev < -5 and pe is not None and pe >= 30:
        return f"매출 역성장에 PER {pe:.0f}배 — 비싸게 사면 물린다"
    # Revenue declining but still profitable
    if rev is not None and rev < -5 and margin is not None and margin >= 15:
        return f"매출이 빠지는데 마진 {margin:.0f}%는 아직 버티는 중"
    # High growth + reasonable valuation
    if rev is not None and rev >= 20 and pe is not None and 0 < pe < 25:
        return f"매출 {rev:.0f}% 성장에 PER {pe:.0f}배면 아직 매력 있다"
    # High debt danger
    if de is not None and de >= 200 and margin is not None and margin < 10:
        return f"빚이 많고 수익성도 낮다 — 재무 리스크 주의"
    # ROE monster
    if roe is not None and roe >= 25:
        return f"ROE {roe:.0f}%로 돈 버는 효율이 최상급"
    # Pure growth story
    if rev is not None and rev >= 30:
        return f"매출 {rev:.0f}% 급성장 — 성장주 테마"
    # Expensive but nothing else stands out
    if pe is not None and pe >= 50:
        return f"PER {pe:.0f}배 — 시장이 미래에 크게 베팅 중"
    # Cheap
    if pe is not None and 0 < pe < 10:
        return f"PER {pe:.0f}배로 바닥권 — 이유가 있는지 확인 필요"
    # Nothing dramatic
    if margin is not None and margin >= 10:
        return "수익성은 괜찮은 회사"
    return ""


def build_plain_summary(snapshot: FundamentalSnapshot) -> str:
    """Convert financial metrics to a natural Korean paragraph with a punchy edge."""
    margin = _pct(snapshot.operating_margin)
    roe = _pct(snapshot.return_on_equity)
    de = snapshot.debt_to_equity
    pe = snapshot.forward_pe
    rev = _pct(snapshot.revenue_growth)

    # Punchy one-liner edge
    edge = _pick_edge(rev, margin, roe, de, pe)

    # Build a flowing paragraph, not a bullet list
    parts: list[str] = []

    # Lead with revenue growth if available — sets the story
    if rev is not None:
        parts.append(_rev_phrase(rev))

    if margin is not None:
        parts.append(_margin_phrase(margin))

    if roe is not None:
        parts.append(_roe_phrase(roe))

    if de is not None:
        parts.append(_debt_phrase(de))

    if pe is not None and pe > 0:
        parts.append(_pe_phrase(pe))

    if not parts:
        return "데이터 부족"

    # Join as natural paragraph with periods
    summary = ". ".join(p.rstrip("., ") for p in parts) + "."

    analyst = build_analyst_summary(snapshot)
    if analyst and "없어요" not in analyst:
        summary += " " + analyst

    if edge:
        return f"**{edge}**\n\n{summary}"
    return summary


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
