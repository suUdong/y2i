from __future__ import annotations

from collections import Counter

from .models import VideoType

# Priority-ordered rules: first match wins for primary type.
# Each rule: (VideoType, title_keywords, description_keywords)
VIDEO_TYPE_RULES: list[tuple[VideoType, list[str], list[str]]] = [
    (
        VideoType.MARKET_REVIEW,
        ["마감시황", "밤사이", "밤시황", "아침에 투자", "클로징벨", "마켓리뷰", "시황정리", "장마감"],
        ["시황", "마감", "지수", "나스닥", "코스피"],
    ),
    (
        VideoType.MACRO,
        ["fomc", "fed", "금리", "환율", "채권", "원자재", "파월", "cpi", "고용지표", "인플레이션", "통화정책", "기준금리"],
        ["금리", "환율", "채권", "fomc", "cpi"],
    ),
    (
        VideoType.EXPERT_INTERVIEW,
        ["대표", "이사", "교수", "박사", "위원", "인터뷰", "라이브", "대담", "특별출연"],
        ["인터뷰", "대담"],
    ),
    (
        VideoType.STOCK_PICK,
        ["종목", "주식", "수혜주", "매수", "사세요", "주가 전망", "종목은", "목표가", "저평가"],
        ["종목", "매수", "목표가"],
    ),
    (
        VideoType.SECTOR,
        ["반도체", "2차전지", "배터리", "바이오", "조선", "정유", "자동차", "방산", "전력인프라", "데이터센터", "ai 반도체"],
        ["섹터", "산업"],
    ),
    (
        VideoType.NEWS_EVENT,
        ["속보", "긴급", "이슈", "전쟁", "관세", "트럼프", "선거", "정치", "탄핵", "계엄"],
        ["속보", "긴급", "이슈"],
    ),
]

# Legacy bucket mapping for backward compatibility
TITLE_BUCKETS = {
    "투자종목": ["종목", "주식", "수혜주", "매수", "사세요", "주가 전망", "종목은"],
    "매크로": ["fomc", "fed", "금리", "환율", "채권", "원자재", "파월", "cpi", "고용지표"],
    "경제전망": ["경제", "전망", "침체", "회복", "버블", "불황", "성장률", "한국경제"],
    "산업분석": ["반도체", "2차전지", "배터리", "바이오", "조선", "정유", "자동차", "방산"],
    "시장리뷰": ["마감시황", "아침", "클로징벨", "나스닥", "s&p500", "다우", "미국증시", "시장"],
    "전문가인터뷰": ["대표", "이사", "교수", "박사", "위원", "라이브", "인터뷰"],
}


def classify_video_type(
    title: str,
    description: str = "",
    tags: list[str] | None = None,
) -> VideoType:
    """Classify a video into a single primary VideoType based on title/description/tags."""
    tags = tags or []
    lower_title = title.lower()
    lower_desc = f"{description} {' '.join(tags)}".lower()

    for video_type, title_keywords, _desc_keywords in VIDEO_TYPE_RULES:
        if any(kw in lower_title for kw in title_keywords):
            return video_type

    # Fallback: check description/tags alone with higher threshold
    for video_type, _title_keywords, desc_keywords in VIDEO_TYPE_RULES:
        desc_hits = sum(1 for kw in desc_keywords if kw in lower_desc)
        if desc_hits >= 2:
            return video_type

    return VideoType.OTHER


def classify_title(title: str) -> list[str]:
    """Legacy bucket classifier - returns list of Korean label strings."""
    lower = title.lower()
    labels = [label for label, keywords in TITLE_BUCKETS.items() if any(keyword in lower for keyword in keywords)]
    return labels or ["기타"]


def summarize_title_classes(titles: list[str]) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for title in titles:
        for label in classify_title(title):
            counter[label] += 1
    return dict(counter)
