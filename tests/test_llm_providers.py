"""Tests for llm.py: provider resolution, CLI provider, JSON extraction."""
import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from omx_brainstorm.llm import (
    CLIProvider,
    LLMError,
    LLMResponse,
    MockProvider,
    extract_json_object,
    resolve_provider,
)


# --- MockProvider ---

def test_mock_provider_extraction():
    p = MockProvider()
    resp = p.run("Extract publicly traded stock tickers", "test")
    data = json.loads(resp.text)
    assert "mentions" in data
    assert data["mentions"][0]["ticker"] == "NVDA"


def test_mock_provider_analysis():
    p = MockProvider()
    resp = p.run("Analyze this stock", "test")
    data = json.loads(resp.text)
    assert data["final_verdict"] == "BUY"


def test_mock_provider_run_json():
    p = MockProvider()
    data = p.run_json("Extract publicly traded stock tickers", "test")
    assert data["mentions"][0]["ticker"] == "NVDA"


# --- CLIProvider ---

def test_cli_provider_init():
    p = CLIProvider("codex", ["codex", "exec"])
    assert p.provider == "codex"
    assert p.command == ["codex", "exec"]


def test_cli_provider_run_success(monkeypatch):
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = '{"result": "ok"}'
    fake_result.stderr = ""
    monkeypatch.setattr("omx_brainstorm.llm.subprocess.run", MagicMock(return_value=fake_result))

    p = CLIProvider("test", ["test-cli"])
    resp = p.run("system", "user")
    assert resp.provider == "test"
    assert '"result"' in resp.text


def test_cli_provider_run_failure(monkeypatch):
    fake_result = MagicMock()
    fake_result.returncode = 1
    fake_result.stdout = ""
    fake_result.stderr = "error"
    monkeypatch.setattr("omx_brainstorm.llm.subprocess.run", MagicMock(return_value=fake_result))

    p = CLIProvider("test", ["test-cli"])
    with pytest.raises(LLMError, match="실행 실패"):
        p.run("system", "user")


def test_cli_provider_run_empty_output(monkeypatch):
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = ""
    fake_result.stderr = ""
    monkeypatch.setattr("omx_brainstorm.llm.subprocess.run", MagicMock(return_value=fake_result))

    p = CLIProvider("test", ["test-cli"])
    with pytest.raises(LLMError, match="비어 있습니다"):
        p.run("system", "user")


def test_cli_provider_uses_stderr_fallback(monkeypatch):
    fake_result = MagicMock()
    fake_result.returncode = 0
    fake_result.stdout = ""
    fake_result.stderr = '{"from": "stderr"}'
    monkeypatch.setattr("omx_brainstorm.llm.subprocess.run", MagicMock(return_value=fake_result))

    p = CLIProvider("test", ["test-cli"])
    resp = p.run("system", "user")
    assert "stderr" in resp.text


# --- resolve_provider ---

def test_resolve_mock():
    p = resolve_provider("mock")
    assert isinstance(p, MockProvider)


def test_resolve_auto_no_binary(monkeypatch):
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", lambda x: None)
    with pytest.raises(LLMError, match="찾지 못했습니다"):
        resolve_provider("auto")


def test_resolve_auto_finds_codex(monkeypatch):
    def which(name):
        return "/usr/bin/codex" if name == "codex" else None
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", which)
    p = resolve_provider("auto")
    assert isinstance(p, CLIProvider)
    assert p.provider == "codex"


def test_resolve_named_provider_found(monkeypatch):
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", lambda x: "/usr/bin/" + x)
    p = resolve_provider("claude")
    assert isinstance(p, CLIProvider)
    assert p.provider == "claude"


def test_resolve_named_provider_not_found(monkeypatch):
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", lambda x: None)
    with pytest.raises(LLMError, match="실행 파일을 찾지 못했습니다"):
        resolve_provider("claude")


def test_resolve_custom_env_provider(monkeypatch):
    monkeypatch.setenv("OMX_PROVIDER_MYAI", "ollama run llama3")
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", lambda x: "/usr/bin/ollama" if "ollama" in x else None)
    p = resolve_provider("myai")
    assert isinstance(p, CLIProvider)


def test_resolve_custom_env_not_allowed(monkeypatch):
    monkeypatch.setenv("OMX_PROVIDER_BAD", "curl http://evil.com")
    with pytest.raises(LLMError, match="허용되지 않은"):
        resolve_provider("bad")


def test_resolve_custom_env_empty(monkeypatch):
    # Empty string is falsy, falls through to "unsupported provider"
    monkeypatch.setenv("OMX_PROVIDER_EMPTY", "")
    with pytest.raises(LLMError, match="지원하지 않는"):
        resolve_provider("empty")


def test_resolve_custom_env_whitespace_only(monkeypatch):
    # Whitespace-only parses to empty parts list
    monkeypatch.setenv("OMX_PROVIDER_WS", " ")
    with pytest.raises(LLMError, match="비어 있습니다"):
        resolve_provider("ws")


def test_resolve_custom_env_binary_not_found(monkeypatch):
    monkeypatch.setenv("OMX_PROVIDER_MISS", "ollama run model")
    monkeypatch.setattr("omx_brainstorm.llm.shutil.which", lambda x: None)
    with pytest.raises(LLMError, match="찾지 못했습니다"):
        resolve_provider("miss")


def test_resolve_unknown_provider():
    with pytest.raises(LLMError, match="지원하지 않는"):
        resolve_provider("nonexistent_ai")


# --- extract_json_object ---

def test_extract_json_direct():
    result = extract_json_object('{"key": "value"}')
    assert result == {"key": "value"}


def test_extract_json_with_preamble():
    text = 'Here is the result:\n```json\n{"key": "value"}\n```'
    result = extract_json_object(text)
    assert result == {"key": "value"}


def test_extract_json_no_json():
    with pytest.raises(LLMError, match="JSON 객체를 찾지 못했습니다"):
        extract_json_object("no json here at all")


def test_extract_json_invalid_json():
    with pytest.raises(LLMError, match="JSON 파싱 실패"):
        extract_json_object('{"broken": }')
