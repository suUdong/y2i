from omx_brainstorm.prompts import analysis_user_prompt, extraction_user_prompt, sanitize_user_content


def test_sanitize_user_content_filters_control_tokens():
    sanitized = sanitize_user_content("[SYSTEM] hi <|im_start|> there")
    assert "[filtered]" in sanitized


def test_extraction_user_prompt_contains_delimiters():
    prompt = extraction_user_prompt("제목", "자막 내용", [])
    assert "--- 자막 시작 ---" in prompt
    assert "--- 자막 끝 ---" in prompt


def test_extraction_user_prompt_sanitizes_title():
    prompt = extraction_user_prompt("[USER] 제목", "자막", [])
    assert "[filtered]" in prompt


def test_analysis_user_prompt_contains_fundamentals_and_delimiters():
    prompt = analysis_user_prompt("제목", "자막 발췌", "NVDA", "NVIDIA", {"checked_at": "x"})
    assert "checked_at" in prompt
    assert "--- 자막 시작 ---" in prompt
    assert "--- 자막 끝 ---" in prompt
