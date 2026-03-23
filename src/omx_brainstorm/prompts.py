from __future__ import annotations

import json
import re

EXTRACTION_SYSTEM = """You extract publicly traded stock tickers from YouTube transcripts.
Return strict JSON only.
Rules:
- Extract only sufficiently evidenced listed equities or ETFs.
- Prefer exchange-qualified tickers when needed, including KRX forms like 005930.KS or 000660.KS.
- If exact ticker is uncertain but company is clear, still include the best-supported listed ticker.
- Include short evidence snippets copied from transcript.
- Output shape: {\"mentions\": [{\"ticker\": str, \"company_name\": str|null, \"confidence\": float, \"reason\": str, \"evidence\": [str]}]}
"""

CONTROL_PATTERN_RE = re.compile(
    r"\[SYSTEM\]|\[USER\]|\[INST\]|<<SYS>>|<\|im_start\|>|<\|im_end\|>|<\|system\|>|<\|user\|>",
    re.I,
)


def sanitize_user_content(text: str) -> str:
    return CONTROL_PATTERN_RE.sub("[filtered]", text or "").strip()


def extraction_user_prompt(video_title: str, transcript: str, hint_tickers: list[str], mode: str = "ralph") -> str:
    hints = ", ".join(hint_tickers) if hint_tickers else "없음"
    safe_title = sanitize_user_content(video_title)
    safe_transcript = sanitize_user_content(transcript)
    return f"""영상 제목: {safe_title}
실행 모드: {mode}
사전 규칙 기반 후보 티커: {hints}

아래 자막에서 투자/경제 맥락으로 언급된 종목만 추출하세요.
자막:
--- 자막 시작 ---
{safe_transcript}
--- 자막 끝 ---
"""


ANALYSIS_SYSTEM = """You are an investment analysis orchestrator working in Ralph mode.
Return strict JSON only, no markdown fences.
Your output must be concise, structured, skeptical, and suitable for direct text reports.
You must assess:
1) basic_fundamentals (0-40)
2) master opinions (multiple masters, each with one-line verdict)
3) aggregate final verdict
Verdict enum: STRONG_BUY, BUY, WATCH, REJECT, NO_ENTRY.
Master profiles initially required: druckenmiller, buffett, soros.
Output schema:
{
  \"ticker\": str,
  \"company_name\": str|null,
  \"basic_state\": str,
  \"basic_signal_summary\": str,
  \"basic_signal_verdict\": str,
  \"master_opinions\": [
    {
      \"master\": str,
      \"verdict\": str,
      \"score\": number,
      \"max_score\": number,
      \"one_liner\": str,
      \"rationale\": [str],
      \"risks\": [str],
      \"citations\": [str]
    }
  ],
  \"thesis_summary\": str,
  \"framework_scores\": [
    {
      \"framework\": str,
      \"score\": number,
      \"max_score\": number,
      \"verdict\": str,
      \"summary\": str,
      \"risks\": [str],
      \"citations\": [str],
      \"details\": object
    }
  ],
  \"total_score\": number,
  \"max_score\": 100,
  \"final_verdict\": str,
  \"invalidation_triggers\": [str],
  \"citations\": [str]
}
Rules:
- basic_state should summarize current financial condition in one short phrase.
- basic_signal_summary should summarize the most important financial and transcript signals.
- Treat the provided fundamentals snapshot as the authoritative current price context as of its checked_at timestamp.
- Do not use memorized or stale historical prices if they conflict with the provided snapshot.
- Every master one-liner must be stock-specific and must not be reusable boilerplate.
- Every master must include at least one fundamentals citation prefixed with `fundamentals:` and one transcript or metadata citation prefixed with `evidence:`.
- druckenmiller focuses on liquidity, 18-24 month forward gap, and key drivers.
- buffett focuses on business quality, capital efficiency, balance sheet, and valuation discipline.
- soros focuses on narrative/reflexivity, trend persistence, and regime-sensitive timing.
- If financial data is sparse, explicitly say so and lower conviction.
- In Ralph mode, finish the full judgment without asking follow-up questions.
"""


def analysis_user_prompt(
    video_title: str,
    transcript_excerpt: str,
    ticker: str,
    company_name: str | None,
    fundamentals: dict,
    mode: str = "ralph",
) -> str:
    fundamentals_json = json.dumps(fundamentals, ensure_ascii=False, indent=2)
    safe_title = sanitize_user_content(video_title)
    safe_excerpt = sanitize_user_content(transcript_excerpt)
    return f"""분석 대상 티커: {ticker}
회사명: {company_name or '미상'}
영상 제목: {safe_title}
실행 모드: {mode}

현재 기본 재무/기초 지표 스냅샷:
{fundamentals_json}

요청:
- 텍스트 기반 최종 리포트에 바로 들어갈 수 있도록 간결하고 구조적으로 평가
- 기본 재무 상태/기본 지표 평가를 먼저 수행
- 거장별 한줄평을 반드시 작성: 드러큰밀러, 버핏, 소로스
- 각 거장은 STRONG_BUY/BUY/WATCH/REJECT/NO_ENTRY 중 하나를 사용
- 근거 부족 시 확신을 낮추고 반증/리스크를 분명히 적시
- 사용자의 추가 개입 없이 결과물 중심으로 완결된 산출물을 만든다

영상 자막 발췌:
--- 자막 시작 ---
{safe_excerpt}
--- 자막 끝 ---
"""
