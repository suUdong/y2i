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
