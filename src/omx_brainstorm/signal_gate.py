from __future__ import annotations

import re

from .macro_signals import extract_macro_signals, indirect_macro_mentions
from .models import VideoSignalAssessment
from .stock_registry import COMPANY_PATTERNS, resolve_kr_ticker
from .title_taxonomy import classify_video_type

_TOKEN_RE = re.compile(r"[0-9A-Za-z가-힣&]+")

FINANCE_KEYWORDS = {
    '투자', '종목', '실적', '반도체', '메모리', '파운드리', '엔비디아', '삼성', 'sk하이닉스', '로드맵', '수혜주',
    '소부장', '밸류체인', 'cpo', 'ai', '공정', '장비', '소재', '전략', '매출', '이익', 'capex', 'hbm', 'gpu',
    '전력', '전력인프라', '데이터센터', '변압기', '그리드', '버블',
    '코스피', '코스닥', '증시', '시황', '국장', '밸류업', '저pbr', '저pbr주', '리레이팅',
    '2차전지', '바이오', '방산', '조선', '전력기기', '전선', '원전', '로봇', '지주사',
    '주도주', '관련주', '상한가', '수급', '공매도', '관세'
}
NON_EQUITY_KEYWORDS = {'브이로그', '먹방', '여행', '일상', '운동', '게임', '리뷰만', '광고'}
GENERIC_TITLE_CUES = {
    '폭락', '현금', '조심하세요', '개인투자자', '이 실수', '이 주식만', '부자되는', '위험 신호', '위험신호',
    '희비', '이상 현상', '기회 왔다', '사야 할 것들', '지금 사야',
    '인터뷰', '대담', '토론', '라이브',
}
ACTIONABLE_TITLE_ANCHORS = {
    '반도체', '메모리', '전쟁', '방산', '금리', '환율', '유가',
    '건설', '부동산', '증권', '조선', '정유', 'ai 반도체', '데이터센터', 'gpu',
    '2차전지', '바이오', '전력기기', '원전', '밸류업', '저pbr', '지주사',
    '관세전쟁', '무역전쟁', '트럼프',
}
EXPLICIT_SECTOR_CUES = {
    '건설', '건설주', '성장주', '증권', '증권주', '부동산', '반도체', '메모리', '파운드리',
    '방산', '방산주', '조선', '조선주', '정유', '정유주', '원전', '전력', '전력기기',
    '데이터센터', '수혜주', '밸류체인', '은행', '은행주', '보험', '보험주', '통신', '통신주',
    '유틸리티', '화학', '화학주', '항공', '항공주', '수출주', '지주사', '로봇', '바이오',
    '2차전지', '배터리', '리츠', '업종', '섹터',
}
GENERIC_INDIRECT_ONLY_MACRO_SIGNALS = {"semiconductor_cycle", "ai_theme"}
NO_TICKER_SKIP_REASON = '종목 추출 근거가 부족해 종목 분석을 건너뜀'


def downgrade_signal_without_tickers(
    assessment: VideoSignalAssessment,
    *,
    reason: str = NO_TICKER_SKIP_REASON,
) -> VideoSignalAssessment:
    metrics = dict(assessment.metrics or {})
    metrics["extracted_ticker_count"] = 0
    metrics["post_extraction_gate"] = "downgraded_no_tickers"
    return VideoSignalAssessment(
        signal_score=min(float(assessment.signal_score), 54.0),
        video_signal_class="LOW_SIGNAL",
        should_analyze_stocks=False,
        reason=reason,
        skip_reason=reason,
        video_type=assessment.video_type,
        metrics=metrics,
    )


def _count_spaced_kr_company_hits(text: str) -> int:
    tokens = [match.group(0).lower() for match in _TOKEN_RE.finditer(text)]
    hits = 0
    idx = 0
    while idx < len(tokens):
        matched = False
        for window in range(min(3, len(tokens) - idx), 1, -1):
            candidate = "".join(tokens[idx : idx + window])
            if resolve_kr_ticker(candidate) is None:
                continue
            hits += 1
            idx += window
            matched = True
            break
        if not matched:
            idx += 1
    return hits


def assess_video_signal(
    title: str,
    transcript_text: str,
    description: str = "",
    tags: list[str] | None = None,
    transcript_source: str | None = None,
) -> VideoSignalAssessment:
    tags = tags or []
    metadata_text = " ".join(part for part in [title, description, " ".join(tags)] if part)
    title_description_text = f"{title} {description}".lower()
    title_only_text = title.lower()
    signal_body = transcript_text[:20000] if transcript_text else metadata_text
    text = f"{title} {signal_body} {description} {' '.join(tags)}".lower()
    finance_hits = sum(1 for kw in FINANCE_KEYWORDS if kw in text)
    non_equity_hits = sum(1 for kw in NON_EQUITY_KEYWORDS if kw in text)
    company_hits = sum(1 for pattern in COMPANY_PATTERNS if pattern.search(text)) + _count_spaced_kr_company_hits(text)
    title_company_hits = sum(1 for pattern in COMPANY_PATTERNS if pattern.search(title_only_text)) + _count_spaced_kr_company_hits(title_only_text)
    title_description_company_hits = sum(1 for pattern in COMPANY_PATTERNS if pattern.search(title_description_text)) + _count_spaced_kr_company_hits(title_description_text)
    transcript_len = len(transcript_text)
    metadata_company_hits = sum(1 for pattern in COMPANY_PATTERNS if pattern.search(metadata_text)) + _count_spaced_kr_company_hits(metadata_text.lower())
    used_metadata_fallback = (
        (transcript_source or "").lower().endswith("metadata_fallback")
        or (transcript_len == 0 and bool(metadata_text))
    )
    macro_signals = extract_macro_signals(text)
    macro_signal_count = len(macro_signals)
    actionable_macro_count = sum(1 for signal in macro_signals if signal["actionable"])
    macro_sector_count = len({sector for signal in macro_signals for sector in signal["beneficiary_sectors"]})
    macro_stock_candidates = len([item for item in indirect_macro_mentions(title, transcript_text or metadata_text) if item.confidence >= 0.55])
    has_actionable_anchor = any(keyword in title_description_text for keyword in ACTIONABLE_TITLE_ANCHORS)
    title_has_actionable_anchor = any(keyword in title_only_text for keyword in ACTIONABLE_TITLE_ANCHORS)
    has_generic_title_cue = any(keyword in title_description_text for keyword in GENERIC_TITLE_CUES)
    has_explicit_sector_path = any(keyword in title_description_text for keyword in EXPLICIT_SECTOR_CUES)
    has_only_generic_indirect_macro_path = (
        macro_stock_candidates >= 1
        and title_description_company_hits == 0
        and company_hits == 0
        and macro_signal_count > 0
        and all(signal["name"] in GENERIC_INDIRECT_ONLY_MACRO_SIGNALS for signal in macro_signals)
    )
    has_macro_only_without_sector_path = (
        macro_stock_candidates >= 2
        and title_description_company_hits == 0
        and company_hits == 0
        and actionable_macro_count >= 1
        and not has_explicit_sector_path
    )
    has_title_named_company_path = title_company_hits >= 1
    has_repeated_company_path = company_hits >= 2
    has_metadata_named_equity_path = used_metadata_fallback and title_description_company_hits >= 1 and finance_hits >= 3
    has_macro_stock_path = (
        macro_stock_candidates >= 2
        and not has_only_generic_indirect_macro_path
        and not used_metadata_fallback
        and has_explicit_sector_path
        and (has_actionable_anchor or title_description_company_hits >= 1)
    )
    has_specific_stock_path = (
        has_title_named_company_path
        or has_repeated_company_path
        or has_metadata_named_equity_path
        or has_macro_stock_path
    )

    score = 0.0
    score += min(finance_hits * 4, 40)
    score += min(company_hits * 8, 40)
    score += min(macro_signal_count * 10, 20)
    score += 10 if transcript_len > 3000 else 5 if transcript_len > 1000 else 0
    score -= min(non_equity_hits * 15, 40)
    if used_metadata_fallback and finance_hits >= 3:
        score = max(score, 35.0)
    if used_metadata_fallback and title_description_company_hits >= 1 and finance_hits >= 3:
        score = max(score, 55.0)
    if title_description_company_hits >= 1 and finance_hits >= 2:
        score = max(score, 55.0)
    if finance_hits >= 6 and has_actionable_anchor:
        score = max(score, 55.0)
    if has_actionable_anchor and actionable_macro_count >= 1 and finance_hits >= 3:
        score = max(score, 55.0)
    if has_actionable_anchor and actionable_macro_count >= 1 and has_specific_stock_path:
        score = max(score, 70.0)
    if title_description_company_hits >= 2 and finance_hits >= 4:
        score = max(score, 70.0)
    if has_generic_title_cue and title_description_company_hits == 0 and company_hits == 0:
        score = min(score, 54.0)
    if has_generic_title_cue and not has_actionable_anchor:
        score = min(score, 54.0)
    if has_generic_title_cue and not title_has_actionable_anchor and title_company_hits == 0:
        score = min(score, 54.0)
    if has_macro_only_without_sector_path:
        score = min(score, 54.0)
    score = max(0.0, min(100.0, score))

    if non_equity_hits >= 2 and finance_hits == 0:
        klass = 'NON_EQUITY'
        should = False
        reason = '주식/산업 분석보다 비주식성 콘텐츠 신호가 강함'
    elif (
        score >= 70 and has_specific_stock_path and not has_only_generic_indirect_macro_path
    ) or (
        score >= 55 and has_actionable_anchor and actionable_macro_count >= 1 and has_specific_stock_path
    ):
        klass = 'ACTIONABLE'
        should = True
        reason = '직접 종목 또는 매크로-섹터-종목 연결까지 포함하면 분석 가치가 높음'
    elif score >= 55 or (used_metadata_fallback and actionable_macro_count >= 1):
        klass = 'SECTOR_ONLY'
        should = has_specific_stock_path
        reason = '섹터 중심이지만 종목 단서가 충분해 분석 가치가 있음'
    elif score >= 35:
        klass = 'LOW_SIGNAL'
        should = False
        reason = '시황/섹터 일반론 위주로 종목 추출 근거가 약함'
    else:
        klass = 'NOISE'
        should = False
        reason = '종목 분석에 활용할 실질 신호가 부족함'

    video_type = classify_video_type(title, description, tags)

    return VideoSignalAssessment(
        signal_score=score,
        video_signal_class=klass,
        should_analyze_stocks=should,
        reason=reason,
        skip_reason="" if should else (
            '섹터 흐름은 유효하지만 구체 종목 연결 근거가 부족해 종목 분석은 건너뜀'
            if klass == 'SECTOR_ONLY'
            else reason
        ),
        video_type=video_type.value,
        metrics={
            'finance_keyword_hits': finance_hits,
            'company_hits': company_hits,
            'non_equity_hits': non_equity_hits,
            'transcript_chars': transcript_len,
            'title_company_hits': title_company_hits,
            'title_description_company_hits': title_description_company_hits,
            'metadata_company_hits': metadata_company_hits,
            'metadata_chars': len(metadata_text),
            'used_metadata_fallback': used_metadata_fallback,
            'macro_signal_count': macro_signal_count,
            'actionable_macro_count': actionable_macro_count,
            'macro_sector_count': macro_sector_count,
            'macro_stock_candidates': macro_stock_candidates,
            'has_actionable_anchor': has_actionable_anchor,
            'title_has_actionable_anchor': title_has_actionable_anchor,
            'has_generic_title_cue': has_generic_title_cue,
            'has_explicit_sector_path': has_explicit_sector_path,
            'has_title_named_company_path': has_title_named_company_path,
            'has_repeated_company_path': has_repeated_company_path,
            'has_metadata_named_equity_path': has_metadata_named_equity_path,
            'has_macro_stock_path': has_macro_stock_path,
            'has_macro_only_without_sector_path': has_macro_only_without_sector_path,
            'has_specific_stock_path': has_specific_stock_path,
            'has_only_generic_indirect_macro_path': has_only_generic_indirect_macro_path,
        },
    )
