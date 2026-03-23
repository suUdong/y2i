from __future__ import annotations

import re

from .models import ExpertInsight
from .stock_registry import COMPANY_MAP

# Patterns for detecting expert name + affiliation in Korean financial YouTube titles/transcripts
# Common patterns: "홍길동 대표", "김교수 박사", "이OO 위원"
TITLE_EXPERT_PATTERNS = [
    # "홍길동 OO증권 대표" or "홍길동 대표"
    re.compile(r"([가-힣]{2,4})\s+([\w가-힣]*(?:증권|자산운용|투자|캐피탈|리서치|경제연구|연구원|대학교?)?)\s*(대표|이사|교수|박사|위원|애널리스트|연구원|센터장|본부장|팀장|소장|원장|이코노미스트)"),
    # "대표 홍길동"
    re.compile(r"(대표|이사|교수|박사|위원|애널리스트|연구원|센터장|본부장|팀장|소장|원장|이코노미스트)\s+([가-힣]{2,4})"),
    # Simpler: name + title alone
    re.compile(r"([가-힣]{2,4})\s*(대표|이사|교수|박사|위원|애널리스트|연구원|센터장|본부장|팀장|소장|원장|이코노미스트)"),
]

AFFILIATION_PATTERNS = [
    re.compile(r"([가-힣A-Za-z]{2,15}(?:증권|자산운용|투자|캐피탈|리서치|경제연구소|연구원|대학교?|금융|은행))"),
]

BULLISH_CUES = {"상승", "반등", "강세", "매수", "좋다", "긍정", "호재", "기대", "수혜", "유망", "추천", "사세요", "올라"}
BEARISH_CUES = {"하락", "약세", "매도", "위험", "리스크", "부정", "악재", "경계", "조심", "팔아", "내려"}

CLAIM_SENTENCE_CUES = {
    "전망", "예상", "판단", "생각", "봐야", "될 것", "갈 것", "해야", "중요",
    "핵심", "주목", "관건", "변수", "포인트", "시나리오", "가능성", "확률",
    "매수", "매도", "추천", "유망", "수혜", "리스크", "위험", "기회",
}


def extract_expert_insights(
    title: str,
    text: str,
    description: str = "",
) -> list[ExpertInsight]:
    """Extract expert name, affiliation, key claims from interview-type content."""
    combined = f"{title} {description}"

    experts = _extract_experts_from_text(combined)
    if not experts:
        experts = _extract_experts_from_text(text[:3000])
    if not experts:
        return []

    claims = _extract_key_claims(text)
    sentiment = _detect_sentiment(text)
    topic = _detect_topic(title, text)
    mentioned_tickers = _extract_mentioned_tickers(f"{title} {text}")

    return [
        ExpertInsight(
            expert_name=name,
            affiliation=affiliation,
            key_claims=claims[:5],
            topic=topic,
            sentiment=sentiment,
            mentioned_tickers=mentioned_tickers[:5],
        )
        for name, affiliation in experts
    ]


def _extract_experts_from_text(text: str) -> list[tuple[str, str]]:
    """Extract (name, affiliation) pairs from text."""
    experts: list[tuple[str, str]] = []
    seen_names: set[str] = set()

    for pattern in TITLE_EXPERT_PATTERNS:
        for match in pattern.finditer(text):
            groups = match.groups()
            if len(groups) == 3:
                name, affiliation, _title = groups[0], groups[1], groups[2]
            elif len(groups) == 2:
                # Could be (title, name) or (name, title)
                g0, g1 = groups
                if len(g0) <= 6 and any(c in g0 for c in "대이교박위애연센본팀소원"):
                    name, affiliation = g1, ""
                else:
                    name, affiliation = g0, ""
            else:
                continue

            name = name.strip()
            affiliation = affiliation.strip()
            if name and name not in seen_names and 2 <= len(name) <= 4:
                seen_names.add(name)
                if not affiliation:
                    affiliation = _find_affiliation_nearby(text, name)
                experts.append((name, affiliation))

    return experts[:3]


def _find_affiliation_nearby(text: str, name: str) -> str:
    """Try to find an affiliation mentioned near the expert's name."""
    idx = text.find(name)
    if idx < 0:
        return ""
    window = text[max(0, idx - 30):idx + 30]
    for pattern in AFFILIATION_PATTERNS:
        match = pattern.search(window)
        if match:
            return match.group(1)
    return ""


def _extract_key_claims(text: str) -> list[str]:
    """Extract sentences that look like expert claims or predictions."""
    sentences = re.split(r'[.!?\n]', text)
    claims: list[str] = []
    for sentence in sentences:
        s = sentence.strip()
        if len(s) < 10 or len(s) > 200:
            continue
        s_lower = s.lower()
        if any(cue in s_lower for cue in CLAIM_SENTENCE_CUES):
            claims.append(s)
            if len(claims) >= 8:
                break
    return claims


def _detect_sentiment(text: str) -> str:
    """Simple bullish/bearish sentiment from keyword counting."""
    lower = text.lower()
    bull = sum(1 for cue in BULLISH_CUES if cue in lower)
    bear = sum(1 for cue in BEARISH_CUES if cue in lower)
    if bull > bear + 1:
        return "BULLISH"
    if bear > bull + 1:
        return "BEARISH"
    return "NEUTRAL"


def _detect_topic(title: str, text: str) -> str:
    """Detect the primary topic of the interview."""
    combined = f"{title} {text[:500]}".lower()
    topics = [
        ("반도체", ["반도체", "메모리", "hbm", "파운드리"]),
        ("AI/데이터센터", ["ai", "데이터센터", "gpu", "인공지능"]),
        ("금리/통화정책", ["금리", "fomc", "fed", "통화정책", "기준금리"]),
        ("부동산/건설", ["부동산", "건설", "아파트"]),
        ("에너지", ["유가", "원유", "에너지", "전력"]),
        ("2차전지", ["2차전지", "배터리", "전기차"]),
        ("방산/조선", ["방산", "조선", "국방"]),
        ("시장전망", ["시장", "전망", "증시", "코스피", "나스닥"]),
    ]
    for topic_name, keywords in topics:
        if any(kw in combined for kw in keywords):
            return topic_name
    return "일반"


def _extract_mentioned_tickers(text: str) -> list[str]:
    """Extract ticker symbols mentioned alongside the expert's discussion."""
    lower = text.lower()
    tickers: list[str] = []
    seen: set[str] = set()
    for _key, (ticker, _company) in COMPANY_MAP.items():
        if _key in lower and ticker not in seen:
            seen.add(ticker)
            tickers.append(ticker)
            if len(tickers) >= 5:
                break
    return tickers
