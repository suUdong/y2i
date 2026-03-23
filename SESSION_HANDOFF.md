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
- **llm.py 100% 커버리지**

### 알림/대시보드/LLM 강화 (세션 7)
- **30일 대시보드 생성**: CLI `analyze-channel-30d` 서브커맨드
- **Discord 알림 지원**: `send_discord_message()` + env var `DISCORD_WEBHOOK_URL`
- **통합 알림 디스패치**: `notify_all()` → Telegram + Discord
- **LLM 기반 expert claim 강화**: `StructuredClaim` 모델 + heuristic fallback

### 커버리지/대시보드 강화 (세션 8 — 현재)
- **구조화 주장 대시보드 렌더링** (US-005): direction/confidence/reasoning 표시, fallback 지원
- **backtest_automation.py 100%** (US-006): 60% → 100% (+4 tests)
- **research.py 100%** (US-007): 86% → 100% (+21 tests)
- **heuristic_pipeline.py 100%** (US-008): 94% → 100% (+12 tests)

## 2. 현재 검증 상태
- pytest: **343 passed**
- coverage: **95%** (was 93%)
- 전체 커밋: 35개 (세션 7: +5, 세션 8: +4)
- 100% 커버리지 모듈: heuristic_pipeline, research, backtest_automation, notifications, llm, models, comparison, market_review, signal_gate, app_config 등 20개+

## 3. 주요 파일 구조
```
src/omx_brainstorm/
├── models.py            — VideoType, StructuredClaim, ExpertInsight, MacroInsight 등
├── pipeline.py          — OMXPipeline (LLM 분석 + VideoType 분기 + 병렬 처리 + LLM expert claims)
├── heuristic_pipeline.py — 휴리스틱 분석 + dashboard 어댑터 (100% coverage)
├── expert_interview.py  — 전문가 인사이트 (heuristic + LLM 경로)
├── notifications.py     — Telegram + Discord + notify_all() (100% coverage)
├── scheduler.py         — 스케줄러 (notify_all 통합)
├── reporting.py         — 리포트 + 대시보드 (구조화 주장 렌더링)
├── research.py          — 교차 비디오 랭킹 (100% coverage)
├── backtest_automation.py — 아티팩트 백테스트 (100% coverage)
├── llm.py               — LLM provider (100% coverage)
├── cli.py               — CLI (analyze-channel-30d 포함)
├── app_config.py        — AppConfig (100% coverage)
```

## 4. 중요 결정 사항과 아키텍처 맥락
- 큰 리팩토링보다 **데이터 먼저 보고 점진 확장** 방향
- 삼프로TV는 "콘텐츠 유형 구분 + 유형별 인사이트 추출"이 핵심
- LLM expert claim 추출은 fallback 우선 — LLM 실패 시 heuristic으로 자동 전환
- 알림은 notify_all()로 통합 — 새 채널 추가 시 notify_all만 확장
- 대시보드는 structured_claims 우선 표시, 없으면 key_claims fallback

## 5. 다음 세션에서 할 수 있는 작업

1. **실제 삼프로TV 실행 테스트** (실 YouTube API 연동)
   - `omx-brainstorm analyze-channel-30d sampro` 실행하여 실제 데이터로 검증

2. **cli.py 커버리지 향상** (현재 70%)
   - analyze-channel-30d, analyze-all, run-comparison 핸들러 테스트

3. **pipeline.py 커버리지 향상** (현재 89%)
   - VideoType 분기 에러 처리, analyze_channel_since 테스트

4. **백테스트 자동화 개선**
   - 30일 분석 결과를 자동 백테스트와 연결

5. **스케줄러 Cron 연동**
   - systemd/cron으로 일일 자동 실행 설정

## 6. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`
