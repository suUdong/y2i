from __future__ import annotations

from .errors import OMXError
from .models import MacroInsight, TickerMention
from .stock_registry import SECTOR_STOCKS
from .utils import merge_mention, unique_preserve

MACRO_RULES = [
    {
        "name": "rate_cut",
        "label": "금리 인하",
        "keywords": ["금리 인하", "기준금리 인하", "rate cut", "fed cut", "금리 하락"],
        "beneficiary_sectors": ["growth_tech", "real_estate", "construction", "securities"],
        "actionable": True,
        "base_confidence": 0.62,
    },
    {
        "name": "rate_hike",
        "label": "금리 인상",
        "keywords": ["금리 인상", "고금리", "higher for longer", "금리 상승"],
        "beneficiary_sectors": ["banks", "insurance"],
        "actionable": True,
        "base_confidence": 0.6,
    },
    {
        "name": "strong_dollar",
        "label": "달러 강세",
        "keywords": ["달러 강세", "환율 상승", "원달러 상승", "원화 약세", "dxy 상승"],
        "beneficiary_sectors": ["exporters", "shipbuilding"],
        "actionable": True,
        "base_confidence": 0.58,
    },
    {
        "name": "weak_dollar",
        "label": "달러 약세",
        "keywords": ["달러 약세", "환율 하락", "원달러 하락", "원화 강세", "dxy 하락"],
        "beneficiary_sectors": ["growth_tech", "importers"],
        "actionable": True,
        "base_confidence": 0.56,
    },
    {
        "name": "oil_up",
        "label": "유가 상승",
        "keywords": ["유가 상승", "국제유가 상승", "wti 상승", "브렌트 상승"],
        "beneficiary_sectors": ["refiners", "shipbuilding"],
        "actionable": True,
        "base_confidence": 0.58,
    },
    {
        "name": "oil_down",
        "label": "유가 하락",
        "keywords": ["유가 하락", "국제유가 하락", "wti 하락", "브렌트 하락"],
        "beneficiary_sectors": ["airlines", "chemicals"],
        "actionable": True,
        "base_confidence": 0.56,
    },
    {
        "name": "recovery",
        "label": "경기 회복",
        "keywords": ["경기 회복", "리오프닝", "턴어라운드", "재고 정상화", "경기 반등"],
        "beneficiary_sectors": ["cyclicals", "construction", "exporters"],
        "actionable": False,
        "base_confidence": 0.46,
    },
    {
        "name": "slowdown",
        "label": "경기 둔화",
        "keywords": ["경기 둔화", "침체", "리세션", "소비 둔화", "경기 하강"],
        "beneficiary_sectors": ["defensives", "telecom", "utilities"],
        "actionable": False,
        "base_confidence": 0.4,
    },
    {
        "name": "semiconductor_cycle",
        "label": "반도체 슈퍼사이클",
        "keywords": ["반도체", "메모리", "hbm", "슈퍼사이클", "삼성전자", "sk하이닉스", "주가 전망"],
        "beneficiary_sectors": ["semiconductors", "semicap"],
        "actionable": True,
        "base_confidence": 0.7,
    },
    {
        "name": "ai_theme",
        "label": "AI 구조 변화",
        "keywords": ["ai", "인공지능", "일자리", "gpu", "ai 반도체", "ai 투자"],
        "beneficiary_sectors": ["ai_platforms", "semiconductors"],
        "actionable": False,
        "base_confidence": 0.45,
    },
    {
        "name": "war_defense",
        "label": "전쟁/방산",
        "keywords": ["전쟁", "방산", "한반도", "지정학", "안보"],
        "beneficiary_sectors": ["defense", "shipbuilding"],
        "actionable": True,
        "base_confidence": 0.66,
    },
    {
        "name": "risk_off",
        "label": "주식시장 폭락/위험",
        "keywords": ["폭락", "위험 신호", "현금", "조심하세요", "리스크"],
        "beneficiary_sectors": ["defensives", "telecom", "utilities"],
        "actionable": False,
        "base_confidence": 0.34,
    },
]

def extract_macro_signals(text: str) -> list[dict]:
    """Detect macro and sector-cycle signals from freeform text."""
    lower = text.lower()
    signals = []
    for rule in MACRO_RULES:
        matched = [keyword for keyword in rule["keywords"] if keyword in lower]
        if matched:
            signals.append(
                {
                    "name": rule["name"],
                    "label": rule["label"],
                    "matched_keywords": matched,
                    "beneficiary_sectors": list(rule["beneficiary_sectors"]),
                    "actionable": rule["actionable"],
                    "base_confidence": rule["base_confidence"],
                }
            )
    return signals


def indirect_macro_mentions(video_title: str, transcript_text: str) -> list[TickerMention]:
    """Map macro signals into representative sector-leading stock mentions."""
    signals = extract_macro_signals(f"{video_title} {transcript_text}")
    mentions: dict[str, TickerMention] = {}
    for signal in signals:
        matched_keywords = unique_preserve(signal["matched_keywords"])
        for sector in signal["beneficiary_sectors"][:4]:
            for ticker, company_name in SECTOR_STOCKS.get(sector, [])[:2]:
                confidence = min(0.82, signal["base_confidence"] + min(len(matched_keywords) * 0.04, 0.08))
                tier = "HIGH" if confidence >= 0.68 else "MEDIUM" if confidence >= 0.55 else "LOW"
                reason = f"[{tier}] 매크로 시그널 '{signal['label']}' -> 섹터 '{sector}' 수혜 연결"
                mention = TickerMention(
                    ticker=ticker,
                    company_name=company_name,
                    confidence=confidence,
                    reason=reason,
                    evidence=list(matched_keywords) + [f"tier:{tier}"],
                )
                merge_mention(mentions, mention)
    return sorted(mentions.values(), key=lambda item: (-item.confidence, item.ticker))


# --- Macro Insight Extraction (structured, per-indicator) ---

MACRO_INSIGHT_RULES: list[dict] = [
    {
        "indicator": "interest_rate",
        "label": "금리",
        "up_keywords": ["금리 인상", "고금리", "higher for longer", "금리 상승", "긴축"],
        "down_keywords": ["금리 인하", "기준금리 인하", "rate cut", "fed cut", "금리 하락", "완화"],
        "neutral_keywords": ["금리 동결", "금리 유지", "rate hold"],
        "up_sentiment": "BEARISH",
        "down_sentiment": "BULLISH",
        "up_sectors": ["banks", "insurance"],
        "down_sectors": ["growth_tech", "real_estate", "construction", "securities"],
        "base_confidence": 0.65,
    },
    {
        "indicator": "fx",
        "label": "환율/달러",
        "up_keywords": ["달러 강세", "환율 상승", "원달러 상승", "원화 약세", "dxy 상승"],
        "down_keywords": ["달러 약세", "환율 하락", "원달러 하락", "원화 강세", "dxy 하락"],
        "neutral_keywords": ["환율 안정", "환율 보합"],
        "up_sentiment": "BEARISH",
        "down_sentiment": "BULLISH",
        "up_sectors": ["exporters", "shipbuilding"],
        "down_sectors": ["growth_tech", "importers"],
        "base_confidence": 0.60,
    },
    {
        "indicator": "oil",
        "label": "유가",
        "up_keywords": ["유가 상승", "국제유가 상승", "wti 상승", "브렌트 상승", "유가 급등"],
        "down_keywords": ["유가 하락", "국제유가 하락", "wti 하락", "브렌트 하락", "유가 급락"],
        "neutral_keywords": ["유가 보합", "유가 안정"],
        "up_sentiment": "NEUTRAL",
        "down_sentiment": "NEUTRAL",
        "up_sectors": ["refiners", "shipbuilding"],
        "down_sectors": ["airlines", "chemicals"],
        "base_confidence": 0.60,
    },
    {
        "indicator": "fomc",
        "label": "FOMC/연준",
        "up_keywords": ["매파", "hawkish", "긴축 기조", "양적긴축", "qt"],
        "down_keywords": ["비둘기파", "dovish", "완화 기조", "양적완화", "qe", "피봇"],
        "neutral_keywords": ["fomc", "연준", "파월", "fed"],
        "up_sentiment": "BEARISH",
        "down_sentiment": "BULLISH",
        "up_sectors": ["banks"],
        "down_sectors": ["growth_tech", "real_estate"],
        "base_confidence": 0.62,
    },
    {
        "indicator": "cpi",
        "label": "물가/CPI",
        "up_keywords": ["인플레이션", "물가 상승", "cpi 상승", "물가 급등", "스태그플레이션"],
        "down_keywords": ["디플레이션", "물가 하락", "cpi 하락", "물가 안정", "인플레 둔화"],
        "neutral_keywords": ["cpi", "소비자물가", "물가"],
        "up_sentiment": "BEARISH",
        "down_sentiment": "BULLISH",
        "up_sectors": ["defensives", "utilities"],
        "down_sectors": ["growth_tech", "cyclicals"],
        "base_confidence": 0.58,
    },
    {
        "indicator": "employment",
        "label": "고용/실업",
        "up_keywords": ["고용 호조", "실업률 하락", "일자리 증가", "고용 서프라이즈"],
        "down_keywords": ["고용 둔화", "실업률 상승", "일자리 감소", "고용 쇼크", "해고"],
        "neutral_keywords": ["고용지표", "비농업고용", "실업률", "nfp"],
        "up_sentiment": "BULLISH",
        "down_sentiment": "BEARISH",
        "up_sectors": ["cyclicals", "construction"],
        "down_sectors": ["defensives", "utilities"],
        "base_confidence": 0.55,
    },
    {
        "indicator": "sector_rotation",
        "label": "섹터 로테이션",
        "up_keywords": ["로테이션", "순환매", "갈아타기", "섹터 이동", "자금 이동"],
        "down_keywords": [],
        "neutral_keywords": ["섹터", "업종"],
        "up_sentiment": "NEUTRAL",
        "down_sentiment": "NEUTRAL",
        "up_sectors": ["cyclicals"],
        "down_sectors": [],
        "base_confidence": 0.50,
    },
]


def extract_macro_insights(title: str, text: str) -> list[MacroInsight]:
    """Extract structured macro insights with direction and confidence per indicator."""
    combined = f"{title} {text}".lower()
    insights: list[MacroInsight] = []

    for rule in MACRO_INSIGHT_RULES:
        up_hits = [kw for kw in rule["up_keywords"] if kw in combined]
        down_hits = [kw for kw in rule["down_keywords"] if kw in combined]
        neutral_hits = [kw for kw in rule["neutral_keywords"] if kw in combined]
        all_hits = up_hits + down_hits + neutral_hits

        if not all_hits:
            continue

        # Determine direction based on keyword dominance
        if up_hits and not down_hits:
            direction = "UP"
            sentiment = rule["up_sentiment"]
            sectors = list(rule["up_sectors"])
        elif down_hits and not up_hits:
            direction = "DOWN"
            sentiment = rule["down_sentiment"]
            sectors = list(rule["down_sectors"])
        elif up_hits and down_hits:
            direction = "UP" if len(up_hits) > len(down_hits) else "DOWN" if len(down_hits) > len(up_hits) else "NEUTRAL"
            sentiment = rule["up_sentiment"] if direction == "UP" else rule["down_sentiment"] if direction == "DOWN" else "NEUTRAL"
            sectors = list(rule["up_sectors"] if direction == "UP" else rule["down_sectors"] if direction == "DOWN" else [])
        else:
            direction = "NEUTRAL"
            sentiment = "NEUTRAL"
            sectors = []

        confidence = min(0.95, rule["base_confidence"] + min(len(all_hits) * 0.05, 0.15))

        insights.append(MacroInsight(
            indicator=rule["indicator"],
            direction=direction,
            label=rule["label"],
            confidence=round(confidence, 2),
            matched_keywords=all_hits,
            sentiment=sentiment,
            beneficiary_sectors=sectors,
        ))

    return insights
