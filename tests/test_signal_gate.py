from omx_brainstorm.signal_gate import assess_video_signal


def test_signal_gate_uses_metadata_fallback_for_named_equities():
    result = assess_video_signal(
        title="[아바타] 이게 없으면 GPU도 못 돌려, AI 진화에 필요한 핵심 전력 기술은?",
        transcript_text="",
        description="AI 데이터센터 전력 인프라와 765kV 변압기 수요를 다루며 HD현대일렉트릭과 효성중공업을 언급한다.",
        tags=["전력인프라", "hd현대일렉트릭", "효성중공업", "AI반도체", "투자 분석"],
    )

    assert result.video_signal_class != "NOISE"
    assert result.should_analyze_stocks is True
    assert result.metrics["used_metadata_fallback"] is True


def test_signal_gate_understands_korean_market_sector_keywords():
    result = assess_video_signal(
        title="코스닥 바이오와 2차전지, 지금은 밸류업보다 실적",
        transcript_text="코스닥 바이오와 2차전지 주도주를 점검하고 관련주 실적과 수급을 본다.",
        description="한국 증시 시황과 저PBR, 지주사, 밸류업 흐름 점검",
        tags=["코스닥", "바이오", "2차전지", "밸류업"],
    )

    assert result.metrics["finance_keyword_hits"] >= 6
    assert result.video_signal_class in {"ACTIONABLE", "SECTOR_ONLY"}


def test_signal_gate_keeps_sector_label_but_skips_stock_analysis_without_specific_path(monkeypatch):
    monkeypatch.setattr("omx_brainstorm.signal_gate.indirect_macro_mentions", lambda *args, **kwargs: [])

    result = assess_video_signal(
        title="반도체 수혜주 밸류체인 투자 전략 총정리",
        transcript_text="반도체 투자 전략과 수혜주, 밸류체인, 실적, 매출, 이익, 전략을 반복 점검한다.",
        description="반도체 업황과 투자 전략만 다루는 섹터 영상",
        tags=["반도체", "수혜주", "밸류체인", "투자"],
    )

    assert result.video_signal_class == "SECTOR_ONLY"
    assert result.should_analyze_stocks is False
    assert result.skip_reason


def test_signal_gate_recognizes_spaced_korean_company_names():
    result = assess_video_signal(
        title="SK 하이닉스 실적 점검",
        transcript_text="SK 하이닉스 실적과 반도체 투자 전략을 점검한다.",
        description="메모리 업황과 투자 포인트를 본다.",
        tags=["반도체", "실적", "투자"],
    )

    assert result.video_signal_class in {"ACTIONABLE", "SECTOR_ONLY"}
    assert result.should_analyze_stocks is True
    assert result.metrics["title_description_company_hits"] >= 1
    assert result.metrics["company_hits"] >= 1


def test_signal_gate_keeps_generic_semiconductor_theme_as_sector_only_without_direct_company_path():
    result = assess_video_signal(
        title="반도체 수혜주 총정리, 다음 주도주는?",
        transcript_text=" ".join(
            ["반도체 메모리 hbm ai 데이터센터 투자 수혜주 종목 실적 매출 이익"]
            * 220
        ),
        description="업황과 밸류체인 전반을 훑는 섹터 점검 영상",
        tags=["반도체", "HBM", "AI", "데이터센터", "수혜주"],
    )

    assert result.video_signal_class == "SECTOR_ONLY"
    assert result.should_analyze_stocks is False
    assert result.metrics["macro_stock_candidates"] >= 2
    assert result.metrics["has_only_generic_indirect_macro_path"] is True


def test_signal_gate_blocks_geopolitical_news_without_company_or_sector_path():
    result = assess_video_signal(
        title="트럼프 발표 15분전 누군가가 20조 배팅을 걸었다",
        transcript_text=(
            "트럼프 발표와 중동 전쟁, 환율, 유가, 금리, 증시 충격 가능성을 빠르게 정리한다. "
            "거시 변수와 뉴스 흐름만 다루고 특정 기업이나 개별 투자 전략은 설명하지 않는다."
        ),
        description="전쟁과 거시 뉴스 브리핑",
        tags=["트럼프", "전쟁", "속보"],
    )

    assert result.video_signal_class == "LOW_SIGNAL"
    assert result.should_analyze_stocks is False
    assert result.metrics["macro_stock_candidates"] >= 2
    assert result.metrics["has_explicit_sector_path"] is False
    assert result.metrics["has_specific_stock_path"] is False


def test_signal_gate_keeps_geopolitical_sector_strategy_actionable():
    result = assess_video_signal(
        title="전쟁 장기화 국면, 방산주와 조선주 어디가 더 유리한가",
        transcript_text=(
            "전쟁 장기화와 지정학 리스크 속에서 방산주와 조선주 수혜 흐름을 비교한다. "
            "업황과 수주, 실적, 밸류체인 관점에서 섹터 전략을 설명한다."
        ),
        description="방산주 조선주 섹터 전략",
        tags=["전쟁", "방산", "조선", "수혜주"],
    )

    assert result.video_signal_class == "ACTIONABLE"
    assert result.should_analyze_stocks is True
    assert result.metrics["has_explicit_sector_path"] is True
    assert result.metrics["has_specific_stock_path"] is True


def test_signal_gate_cached_metadata_fallback_does_not_promote_macro_only_video_to_actionable():
    result = assess_video_signal(
        title="[LIVE] 이란 전쟁은 안 끝나고, M7은 끝났다?",
        transcript_text="[LIVE] 이란 전쟁은 안 끝나고, M7은 끝났다? 중동 위기와 시장 충격을 다룬다.",
        description="중동 위기와 시장 충격을 다룬다.",
        tags=["전쟁", "중동", "M7"],
        transcript_source="metadata_fallback",
    )

    assert result.video_signal_class in {"NOISE", "LOW_SIGNAL", "SECTOR_ONLY"}
    assert result.should_analyze_stocks is False
    assert result.metrics["used_metadata_fallback"] is True
    assert result.metrics["has_specific_stock_path"] is False


def test_signal_gate_blocks_single_metadata_company_hit_for_non_stock_topic():
    result = assess_video_signal(
        title="난이도 조절에 실패했다는 2026 수능 영어",
        transcript_text="영어 시험 난이도와 학습법을 설명한다.",
        description="0:00 역대 최고 난이도 영어 12:29 삼성, 국내해외법인간 문서도 영어만 쓴다",
        tags=["슈카", "경제", "시사", "주식"],
    )

    assert result.video_signal_class in {"NOISE", "LOW_SIGNAL", "SECTOR_ONLY"}
    assert result.should_analyze_stocks is False
    assert result.metrics["company_hits"] == 1
    assert result.metrics["has_specific_stock_path"] is False


def test_signal_gate_requires_more_than_single_company_hit_for_macro_talk():
    result = assess_video_signal(
        title="코스피 1만 시나리오, 결국 금리에서 갈린다",
        transcript_text="금리, 코스피, 시장 대응과 자산 배분을 설명한다. 카카오는 예시로 한 번만 언급된다.",
        description="거시 환경 토론과 시장 대응 전략 정리",
        tags=["금리", "코스피", "투자"],
    )

    assert result.video_signal_class in {"NOISE", "LOW_SIGNAL", "SECTOR_ONLY"}
    assert result.should_analyze_stocks is False
    assert result.metrics["title_description_company_hits"] == 0
    assert result.metrics["company_hits"] == 1
    assert result.metrics["has_specific_stock_path"] is False


def test_signal_gate_keeps_named_equity_title_actionable():
    result = assess_video_signal(
        title="엔비디아와 삼성전자 반도체 로드맵 점검",
        transcript_text="엔비디아와 삼성전자 실적, HBM, 데이터센터 수요를 함께 본다.",
        description="구체 종목 중심 분석",
        tags=["엔비디아", "삼성전자", "반도체", "투자"],
    )

    assert result.should_analyze_stocks is True
    assert result.metrics["title_company_hits"] >= 1
    assert result.metrics["has_specific_stock_path"] is True
