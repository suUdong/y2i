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
