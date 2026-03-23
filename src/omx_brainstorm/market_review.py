from __future__ import annotations

import re

from .macro_signals import extract_macro_insights
from .models import MarketReviewSummary

INDEX_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("코스피", re.compile(r"코스피\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
    ("코스닥", re.compile(r"코스닥\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
    ("나스닥", re.compile(r"나스닥\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
    ("S&P500", re.compile(r"s&?p\s*500\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
    ("다우", re.compile(r"다우\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
    ("니케이", re.compile(r"니케이\s*(\d[\d,.]*)?(?:\s*(상승|하락|급등|급락|보합|돌파|붕괴))?")),
]

DIRECTION_MAP = {
    "상승": "UP", "급등": "UP", "돌파": "UP",
    "하락": "DOWN", "급락": "DOWN", "붕괴": "DOWN",
    "보합": "NEUTRAL",
}

RISK_EVENT_KEYWORDS = [
    "전쟁", "관세", "무역전쟁", "제재", "지정학", "인플레이션", "스태그플레이션",
    "금융위기", "은행 위기", "디폴트", "채무불이행", "셧다운", "폭락",
    "리세션", "침체", "경기 둔화", "버블", "트럼프", "선거", "탄핵",
]

SECTOR_KEYWORDS = {
    "반도체": "반도체",
    "2차전지": "2차전지/배터리",
    "배터리": "2차전지/배터리",
    "바이오": "바이오/제약",
    "제약": "바이오/제약",
    "조선": "조선",
    "방산": "방산/국방",
    "건설": "건설/부동산",
    "부동산": "건설/부동산",
    "자동차": "자동차",
    "정유": "정유/에너지",
    "에너지": "정유/에너지",
    "금융": "금융",
    "증권": "금융",
    "은행": "금융",
    "전력": "전력인프라",
    "데이터센터": "AI/데이터센터",
    "ai": "AI/데이터센터",
}

BULLISH_CUES = ["상승", "급등", "돌파", "반등", "강세", "랠리", "호재"]
BEARISH_CUES = ["하락", "급락", "붕괴", "약세", "폭락", "악재", "조정"]


def extract_market_review(title: str, text: str) -> MarketReviewSummary:
    """Extract structured market review summary from a market-review type video."""
    combined = f"{title} {text}"
    lower = combined.lower()

    # 1. Extract index mentions
    indices: list[dict[str, str]] = []
    for name, pattern in INDEX_PATTERNS:
        match = pattern.search(lower)
        if match:
            level = match.group(1) or ""
            direction_word = match.group(2) or ""
            direction = DIRECTION_MAP.get(direction_word, "NEUTRAL")
            detail = f"{level} {direction_word}".strip() if level or direction_word else ""
            indices.append({"name": name, "direction": direction, "detail": detail})

    # 2. Overall market direction
    bull_count = sum(1 for cue in BULLISH_CUES if cue in lower)
    bear_count = sum(1 for cue in BEARISH_CUES if cue in lower)
    if bull_count > bear_count + 1:
        direction = "BULLISH"
    elif bear_count > bull_count + 1:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    # 3. Risk events
    risk_events = list(dict.fromkeys(kw for kw in RISK_EVENT_KEYWORDS if kw in lower))

    # 4. Sector focus
    seen_sectors: dict[str, bool] = {}
    sector_focus: list[str] = []
    for kw, sector_label in SECTOR_KEYWORDS.items():
        if kw in lower and sector_label not in seen_sectors:
            seen_sectors[sector_label] = True
            sector_focus.append(sector_label)

    # 5. Key points - extract sentences that contain index or direction keywords
    sentences = re.split(r'[.!?\n]', combined)
    key_point_cues = {"코스피", "코스닥", "나스닥", "s&p", "다우", "상승", "하락", "급등", "급락", "전망", "방향"}
    key_points: list[str] = []
    for sentence in sentences:
        s = sentence.strip()
        if len(s) < 8:
            continue
        s_lower = s.lower()
        if any(cue in s_lower for cue in key_point_cues) and len(key_points) < 5:
            key_points.append(s)

    # 6. Macro insights
    macro_insights = extract_macro_insights(title, text)

    return MarketReviewSummary(
        indices=indices,
        direction=direction,
        risk_events=risk_events,
        sector_focus=sector_focus,
        key_points=key_points,
        macro_insights=macro_insights,
    )


def render_market_review_md(summary: MarketReviewSummary) -> str:
    """Render a MarketReviewSummary as a markdown string."""
    lines = [
        "# 시장리뷰 요약",
        "",
        f"**시장 방향성:** {summary.direction}",
        "",
    ]

    if summary.indices:
        lines.append("## 주요 지수")
        lines.append("")
        lines.append("| 지수 | 방향 | 상세 |")
        lines.append("|---|---|---|")
        for idx in summary.indices:
            lines.append(f"| {idx['name']} | {idx['direction']} | {idx.get('detail', '-')} |")
        lines.append("")

    if summary.risk_events:
        lines.append("## 리스크 이벤트")
        lines.append("")
        for event in summary.risk_events:
            lines.append(f"- {event}")
        lines.append("")

    if summary.sector_focus:
        lines.append("## 섹터 포커스")
        lines.append("")
        for sector in summary.sector_focus:
            lines.append(f"- {sector}")
        lines.append("")

    if summary.macro_insights:
        lines.append("## 매크로 인사이트")
        lines.append("")
        lines.append("| 지표 | 방향 | 센티먼트 | 신뢰도 |")
        lines.append("|---|---|---|---:|")
        for insight in summary.macro_insights:
            lines.append(f"| {insight.label} | {insight.direction} | {insight.sentiment} | {insight.confidence:.2f} |")
        lines.append("")

    if summary.key_points:
        lines.append("## 핵심 포인트")
        lines.append("")
        for point in summary.key_points:
            lines.append(f"- {point}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
