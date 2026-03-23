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

### 품질/성능 개선 (세션 6 — 현재)
- **테스트 커버리지 86% → 94%**: 92개 신규 테스트 (184 → 276)
  - `test_comparison.py` (10) — 품질 스코어카드, 아티팩트 저장, 채널 비교
  - `test_notifications_and_eval.py` (16) — Telegram, Spearman, 검증, 펀더멘털
  - `test_cli_commands.py` (10) — CLI 전체 서브커맨드
  - `test_youtube_extended.py` (15) — 리졸버, 레지스트리, 파싱
  - `test_parallel_and_extensibility.py` (9) — 병렬 분석, 채널 확장성
  - `test_heuristic_dashboard.py` (11) — heuristic→dashboard 어댑터
  - `test_llm_providers.py` (21) — LLM provider 전체 커버리지
- **병렬 처리 최적화**: `ThreadPoolExecutor` 기반 병렬 비디오 분석 (`max_workers` 파라미터)
- **Heuristic→Dashboard 어댑터**: `heuristic_rows_to_reports()` + `render_heuristic_dashboard()`
  - heuristic dict 결과를 VideoAnalysisReport로 변환하여 통합 대시보드 생성 가능
- **llm.py 100% 커버리지**: CLIProvider, resolve_provider, extract_json_object 전체 테스트
- **채널 확장성 검증**: 등록→설정→분석 E2E 테스트 완료

## 2. 현재 검증 상태
- pytest: **276 passed**
- coverage: **94%**
- 전체 커밋: 26개

## 3. 주요 파일 구조
```
src/omx_brainstorm/
├── models.py            — VideoType, ExpertInsight, MacroInsight, MarketReviewSummary 등
├── pipeline.py          — OMXPipeline (LLM 분석 + VideoType 분기 + 병렬 처리)
├── heuristic_pipeline.py — 휴리스틱 분석 + VideoType 분기 + dashboard 어댑터
├── expert_interview.py  — 전문가 인터뷰 인사이트 추출
├── macro_signals.py     — 매크로 시그널/인사이트 추출
├── market_review.py     — 시장리뷰 요약 추출 + MD 렌더링
├── title_taxonomy.py    — VideoType 분류
├── signal_gate.py       — 영상 신호 평가
├── reporting.py         — 개별 리포트 + 통합 대시보드
├── llm.py               — LLM provider (Mock/CLI/auto resolve)
├── cli.py               — CLI 엔트리포인트
├── ...                  — 기타 지원 모듈
```

## 4. 중요 결정 사항과 아키텍처 맥락
- 큰 리팩토링보다 **데이터 먼저 보고 점진 확장** 방향
- 삼프로TV는 "콘텐츠 유형 구분 + 유형별 인사이트 추출"이 핵심
- transcript fetch는 IP-block 이슈 → cache + metadata fallback 유지
- 병렬 처리는 ThreadPoolExecutor (I/O bound 작업에 적합)

## 5. 다음 세션에서 할 수 있는 작업

1. **실제 삼프로TV 30일 재실행 + 대시보드 생성**
   - video_type 분기 + expert/macro/market_review 추출 포함 상태로 재실행
   - `render_heuristic_dashboard()` 사용하여 통합 대시보드 생성

2. **알림 시스템 연동**
   - notifications.py 기반 Telegram/Discord 알림 연결

3. **LLM 기반 expert claim 강화**
   - 현재 규칙 기반 claim 추출 → LLM으로 더 정교한 주장/근거 추출

4. **heuristic_pipeline 나머지 커버리지 (94% → 97%+)**
   - 에러 처리 분기 (lines 88-89, 94-95, 103, 105, 108)

## 6. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`
