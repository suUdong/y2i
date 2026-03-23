# OMX Session Handoff

## Resume rule
If a future session includes messages like:
- "새로운 세션에서 다시 할거야"
- "다음 세션에서 이어서 하자"
- "지금까지 한 것 정리해줘"
- "handoff 남겨줘"

then the agent should:
1. save the current progress summary to this handoff file (update it)
2. preserve key decisions and current blockers
3. record the exact next recommended step
4. ensure the next session can resume work without re-discovery

## Current project
- Workspace: `/home/wdsr88/workspace/y2i`
- Mode: `ralph`
- Operating rule: continue without user intervention unless there is a real blocker

## 1. 현재까지 완료된 작업 전체 요약

### 기본 파이프라인 (세션 1~4)
- YouTube → transcript/metadata → signal gate → ticker extraction → fundamentals → master opinions → ranking → backtest
- transcript cache, metadata fallback, scheduler, healthcheck, JSON logging, Docker, config
- prompt sanitization, provider allowlist, safer runtime degradation

### P1~P3 완료 (세션 5)
- **P1**: VideoType 기반 파이프라인 분기 (pipeline.py + heuristic_pipeline.py)
- **P2**: 전문가 인터뷰 인사이트 구조화 (expert_interview.py)
- **P3**: 종목 + 매크로 시그널 결합 대시보드 (reporting.py)

### 품질/성능 개선 (세션 6)
- **테스트 커버리지 86% → 94%**: 92개 신규 테스트 (184 → 276)
- **병렬 처리 최적화**: `ThreadPoolExecutor` 기반 병렬 비디오 분석
- **Heuristic→Dashboard 어댑터**: `heuristic_rows_to_reports()` + `render_heuristic_dashboard()`
- **llm.py 100% 커버리지**: CLIProvider, resolve_provider, extract_json_object 전체 테스트

### 알림/대시보드/LLM 강화 (세션 7 — 현재)
- **30일 대시보드 생성** (US-001): `run_channel_30d_single.py`에 `render_heuristic_dashboard()` 통합, CLI `analyze-channel-30d` 서브커맨드 추가 (+7 tests)
- **Discord 알림 지원** (US-002): `send_discord_message()` 웹훅 지원, `NotificationConfig`에 `discord_webhook_url` 추가, env var `DISCORD_WEBHOOK_URL` 지원 (+10 tests)
- **통합 알림 디스패치** (US-003): `notify_all()` → Telegram + Discord 동시 발송, scheduler.py + 30d 파이프라인 연동
- **LLM 기반 expert claim 강화** (US-004): `StructuredClaim` 모델, `extract_expert_claims_llm()`, `extract_expert_insights_with_llm()` with heuristic fallback, MockProvider 응답 추가, pipeline 통합 (+11 tests)

## 2. 현재 검증 상태
- pytest: **304 passed**
- 전체 커밋: 31개 (이번 세션 +4)
- 이번 세션 신규 테스트: 28개

## 3. 주요 파일 구조
```
src/omx_brainstorm/
├── models.py            — VideoType, StructuredClaim, ExpertInsight, MacroInsight 등
├── pipeline.py          — OMXPipeline (LLM 분석 + VideoType 분기 + 병렬 처리 + LLM expert claims)
├── heuristic_pipeline.py — 휴리스틱 분석 + VideoType 분기 + dashboard 어댑터
├── expert_interview.py  — 전문가 인사이트 추출 (heuristic + LLM 경로)
├── macro_signals.py     — 매크로 시그널/인사이트 추출
├── market_review.py     — 시장리뷰 요약 추출 + MD 렌더링
├── notifications.py     — Telegram + Discord 알림 + notify_all()
├── scheduler.py         — 스케줄러 (notify_all 통합)
├── reporting.py         — 개별 리포트 + 통합 대시보드
├── llm.py               — LLM provider (Mock/CLI/auto resolve + expert claim mock)
├── cli.py               — CLI 엔트리포인트 (analyze-channel-30d 포함)
├── app_config.py        — AppConfig (discord_webhook_url 포함)
├── ...                  — 기타 지원 모듈
```

## 4. 중요 결정 사항과 아키텍처 맥락
- 큰 리팩토링보다 **데이터 먼저 보고 점진 확장** 방향
- 삼프로TV는 "콘텐츠 유형 구분 + 유형별 인사이트 추출"이 핵심
- transcript fetch는 IP-block 이슈 → cache + metadata fallback 유지
- 병렬 처리는 ThreadPoolExecutor (I/O bound 작업에 적합)
- LLM expert claim 추출은 fallback 우선 — LLM 실패 시 heuristic으로 자동 전환
- 알림은 notify_all()로 통합 — 새 채널 추가 시 notify_all만 확장

## 5. 다음 세션에서 할 수 있는 작업

1. **실제 삼프로TV 실행 테스트** (실 YouTube API 연동)
   - `omx-brainstorm analyze-channel-30d sampro` 실행하여 실제 데이터로 검증
   - Telegram/Discord 알림 실제 발송 확인

2. **heuristic_pipeline 나머지 커버리지 (94% → 97%+)**
   - 에러 처리 분기 (lines 88-89, 94-95, 103, 105, 108)

3. **LLM expert claim 고도화**
   - 현재 MockProvider만 테스트 → 실제 LLM provider로 품질 검증
   - 구조화된 claim을 대시보드에 렌더링

4. **백테스트 자동화 개선**
   - 30일 분석 결과를 자동 백테스트와 연결
   - 성과 추적 대시보드

5. **스케줄러 Cron 연동**
   - systemd/cron으로 일일 자동 실행 설정

## 6. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`
