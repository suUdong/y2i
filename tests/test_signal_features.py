from omx_brainstorm.signal_features import stock_signal_strength


def test_stock_signal_strength_prefers_transcript_backed_evidence():
    transcript_score = stock_signal_strength(
        ticker="NVDA",
        company_name="NVIDIA",
        video_signal_score=74.0,
        mention_count=2,
        master_variance=6.0,
        evidence_snippets=["엔비디아가 제시한 로드맵", "데이터센터 수요가 강하다"],
        evidence_source="transcript_cache",
    )
    metadata_score = stock_signal_strength(
        ticker="NVDA",
        company_name="NVIDIA",
        video_signal_score=74.0,
        mention_count=2,
        master_variance=6.0,
        evidence_snippets=["엔비디아 로드맵"],
        evidence_source="metadata_fallback",
    )

    assert transcript_score > metadata_score
