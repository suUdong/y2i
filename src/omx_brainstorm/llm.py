from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from typing import Sequence


class LLMError(RuntimeError):
    pass


@dataclass(slots=True)
class LLMResponse:
    provider: str
    text: str


class LLMProvider:
    def run_json(self, system_prompt: str, user_prompt: str) -> dict:
        response = self.run(system_prompt, user_prompt)
        return extract_json_object(response.text)

    def run(self, system_prompt: str, user_prompt: str) -> LLMResponse:  # pragma: no cover - interface only
        raise NotImplementedError


class MockProvider(LLMProvider):
    def run(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        if 'extract structured expert claims' in system_prompt.lower():
            payload = {
                "claims": [
                    {
                        "claim": "반도체 업황이 하반기부터 본격 회복될 것으로 전망",
                        "reasoning": "메모리 재고 조정이 마무리 국면이고 AI 서버 수요가 견인",
                        "confidence": 0.85,
                        "direction": "BULLISH",
                    },
                    {
                        "claim": "금리 인하 시기가 예상보다 늦어질 수 있어 주의 필요",
                        "reasoning": "인플레이션 지표가 여전히 목표치 상회",
                        "confidence": 0.7,
                        "direction": "BEARISH",
                    },
                ]
            }
            return LLMResponse(provider="mock", text=json.dumps(payload, ensure_ascii=False))
        if 'extract publicly traded stock tickers' in system_prompt.lower():
            payload = {
                "mentions": [
                    {
                        "ticker": "NVDA",
                        "company_name": "NVIDIA",
                        "confidence": 0.92,
                        "reason": "자막에서 엔비디아와 데이터센터 칩 수요를 반복 언급",
                        "evidence": ["엔비디아가 아직 더 갈 수 있다", "데이터센터 수요가 강하다"],
                    }
                ]
            }
        else:
            payload = {
                "ticker": "NVDA",
                "company_name": "NVIDIA",
                "basic_state": "성장과 수익성은 우수하지만 밸류 부담이 큰 상태",
                "basic_signal_summary": "매출 성장과 마진은 강하지만 높은 기대치와 멀티플이 부담이다.",
                "basic_signal_verdict": "BUY",
                "master_opinions": [
                    {
                        "master": "druckenmiller",
                        "verdict": "BUY",
                        "score": 83,
                        "max_score": 100,
                        "one_liner": "핵심 드라이버인 AI 인프라 수요는 여전히 강하지만 과열 구간 진입 여부를 봐야 한다.",
                        "rationale": ["AI 인프라 CAPEX가 핵심 드라이버", "18개월 forward 수요 가시성이 높음"],
                        "risks": ["유동성 긴축", "AI 투자 피크아웃"],
                        "citations": ["fundamentals:revenue_growth=45.0%", "evidence:데이터센터 수요가 강하다"],
                    },
                    {
                        "master": "buffett",
                        "verdict": "WATCH",
                        "score": 68,
                        "max_score": 100,
                        "one_liner": "사업 질은 매우 높지만 현재 가격이 안전마진을 충분히 주는지는 불확실하다.",
                        "rationale": ["높은 ROE와 마진", "강한 경쟁 우위"],
                        "risks": ["밸류에이션 부담"],
                        "citations": ["fundamentals:return_on_equity=52.0%", "evidence:엔비디아가 아직 더 갈 수 있다"],
                    },
                    {
                        "master": "soros",
                        "verdict": "BUY",
                        "score": 78,
                        "max_score": 100,
                        "one_liner": "강한 내러티브와 수급이 지속될 수 있지만 반사성 꺾임에는 민감해야 한다.",
                        "rationale": ["강한 시장 내러티브", "추세 지속 가능성"],
                        "risks": ["내러티브 붕괴"],
                        "citations": ["fundamentals:fifty_two_week_change=120.0%", "evidence:데이터센터 수요가 강하다"],
                    },
                ],
                "thesis_summary": "AI 인프라 수요 지속이 핵심이지만 기대치가 높아 진입 가격 관리가 중요하다.",
                "framework_scores": [
                    {
                        "framework": "basic_fundamentals",
                        "score": 29,
                        "max_score": 40,
                        "verdict": "PASS",
                        "summary": "성장성과 수익성은 강하나 밸류 부담이 남아 있다.",
                        "risks": ["높은 밸류에이션"],
                        "citations": ["fundamentals"],
                        "details": {"revenue_growth": "strong", "valuation": "rich"},
                    },
                    {
                        "framework": "master_consensus",
                        "score": 47,
                        "max_score": 60,
                        "verdict": "PASS",
                        "summary": "드러큰밀러와 소로스는 우호적이나 버핏은 가격 매력을 더 보수적으로 본다.",
                        "risks": ["합의도는 높지만 과열 경계 필요"],
                        "citations": ["transcript", "fundamentals"],
                        "details": {"agreement": "medium_high"},
                    },
                ],
                "total_score": 76,
                "max_score": 100,
                "final_verdict": "BUY",
                "invalidation_triggers": ["AI 서버 수주 둔화", "가이던스 하향", "유동성 긴축 전환"],
                "citations": ["transcript", "fundamentals"],
            }
        return LLMResponse(provider="mock", text=json.dumps(payload, ensure_ascii=False))


class CLIProvider(LLMProvider):
    def __init__(self, provider: str, command: Sequence[str]):
        self.provider = provider
        self.command = list(command)

    def run(self, system_prompt: str, user_prompt: str) -> LLMResponse:
        prompt = f"[SYSTEM]\n{system_prompt}\n\n[USER]\n{user_prompt}\n"
        proc = subprocess.run(
            self.command,
            input=prompt,
            text=True,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise LLMError(f"{self.provider} CLI 실행 실패 (exit code={proc.returncode})")
        text = proc.stdout.strip() or proc.stderr.strip()
        if not text:
            raise LLMError(f"{self.provider} CLI 응답이 비어 있습니다")
        return LLMResponse(provider=self.provider, text=text)


DEFAULT_PROVIDER_COMMANDS = {
    "codex": ["codex", "exec", "--skip-git-repo-check"],
    "claude": ["claude", "--print"],
    "gemini": ["gemini", "-p"],
}
ALLOWED_BINARIES = {"codex", "claude", "gemini", "ollama"}


def resolve_provider(provider: str) -> LLMProvider:
    if provider == "mock":
        return MockProvider()
    if provider == "auto":
        for name in ("codex", "claude", "gemini"):
            binary = DEFAULT_PROVIDER_COMMANDS[name][0]
            if shutil.which(binary):
                return CLIProvider(name, DEFAULT_PROVIDER_COMMANDS[name])
        raise LLMError("사용 가능한 LLM CLI(codex/claude/gemini)를 찾지 못했습니다")
    if provider in DEFAULT_PROVIDER_COMMANDS:
        binary = DEFAULT_PROVIDER_COMMANDS[provider][0]
        if not shutil.which(binary):
            raise LLMError(f"{provider} 실행 파일을 찾지 못했습니다")
        return CLIProvider(provider, DEFAULT_PROVIDER_COMMANDS[provider])
    env_name = f"OMX_PROVIDER_{provider.upper()}"
    custom = os.getenv(env_name)
    if custom:
        parts = shlex.split(custom)
        if not parts:
            raise LLMError(f"{env_name} 값이 비어 있습니다")
        binary = os.path.basename(parts[0])
        if binary not in ALLOWED_BINARIES:
            raise LLMError(f"허용되지 않은 provider 실행 파일: {binary}")
        if not shutil.which(parts[0]):
            raise LLMError(f"{binary} 실행 파일을 찾지 못했습니다")
        return CLIProvider(provider, parts)
    raise LLMError(f"지원하지 않는 provider: {provider}")


JSON_RE = re.compile(r"\{.*\}", re.S)


def extract_json_object(text: str) -> dict:
    candidate = text.strip()
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        match = JSON_RE.search(candidate)
        if not match:
            raise LLMError("LLM 응답에서 JSON 객체를 찾지 못했습니다")
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError as exc:
            raise LLMError(f"LLM JSON 파싱 실패: {exc}") from exc
