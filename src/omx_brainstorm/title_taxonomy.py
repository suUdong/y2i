from __future__ import annotations

from collections import Counter


TITLE_BUCKETS = {
    "투자종목": ["종목", "주식", "수혜주", "매수", "사세요", "주가 전망", "종목은"],
    "매크로": ["fomc", "fed", "금리", "환율", "채권", "원자재", "파월", "cpi", "고용지표"],
    "경제전망": ["경제", "전망", "침체", "회복", "버블", "불황", "성장률", "한국경제"],
    "산업분석": ["반도체", "2차전지", "배터리", "바이오", "조선", "정유", "자동차", "방산"],
    "시장리뷰": ["마감시황", "아침", "클로징벨", "나스닥", "s&p500", "다우", "미국증시", "시장"],
    "전문가인터뷰": ["대표", "이사", "교수", "박사", "위원", "라이브", "인터뷰"],
}


def classify_title(title: str) -> list[str]:
    lower = title.lower()
    labels = [label for label, keywords in TITLE_BUCKETS.items() if any(keyword in lower for keyword in keywords)]
    return labels or ["기타"]


def summarize_title_classes(titles: list[str]) -> dict[str, int]:
    counter = Counter()
    for title in titles:
        for label in classify_title(title):
            counter[label] += 1
    return dict(counter)
