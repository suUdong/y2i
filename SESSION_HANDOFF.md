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
- YouTube → transcript/metadata → signal gate → ticker extraction → fundamentals → master opinions → ranking → backtest 파이프라인 구축
- transcript cache 영속화 (`.omx/cache/transcripts/`)
- metadata fallback, scheduler, healthcheck, JSON logging, Docker, config 지원
- prompt sanitization, provider allowlist, transcript cache path sanitization, safer runtime degradation

### P1: video_type 기반 파이프라인 분기 (완료)
- `pipeline.py` + `heuristic_pipeline.py`에 VideoType별 분기 구현
- STOCK_PICK/SECTOR → stock only, MARKET_REVIEW → market review, EXPERT_INTERVIEW → expert + macro, MACRO/NEWS/OTHER → macro
- 에러 복원력: 각 추출기 실패 시에도 리포트 생성 계속

### P2: 전문가 인터뷰 인사이트 구조화 (완료)
- `expert_interview.py`: 전문가명/소속/핵심 주장/센티먼트/주제/관련 종목 추출
- 삼프로TV 파이프 구분 패턴 (`| 이름 소속 직함`) 지원

### P3: 종목 + 매크로 시그널 결합 대시보드 (완료)
- `reporting.py`의 `render_combined_dashboard()`: 매크로/시장리뷰/전문가/종목/거장 한줄평 통합
- 매크로 인사이트 중복 제거, 종목 중복 제거

### 이번 세션 추가 완료
- **테스트 커버리지 86% → 92%**: 60개 신규 테스트 추가
  - `test_comparison.py` (10) — 품질 스코어카드, 아티팩트 저장, 채널 비교
  - `test_notifications_and_eval.py` (16) — Telegram, Spearman, 검증, 펀더멘털
  - `test_cli_commands.py` (10) — CLI 전체 서브커맨드
  - `test_youtube_extended.py` (15) — 리졸버, 레지스트리, 파싱
  - `test_parallel_and_extensibility.py` (9) — 병렬 분석, 채널 확장성
- **병렬 처리 최적화**: `pipeline.py`에 `ThreadPoolExecutor` 기반 병렬 비디오 분석
  - `analyze_channel()`, `analyze_channel_since()` → `max_workers` 파라미터
  - 단일 비디오는 자동 sequential fallback
  - 개별 비디오 실패 시 나머지 계속 분석
- **채널 확장성 검증**: 새 채널 등록 → config 로딩 → 분석 실행 E2E 테스트 완료
- 모든 미커밋 파일 정리: 6개 구조적 커밋으로 git 정리

## 2. 현재 검증 상태
- pytest: **244 passed**
- coverage: **92%**
- 전체 커밋: 24개 (세션 1~4 기존 15 + 이전 세션 6 + 이번 세션 3)

## 3. 주요 파일 구조
```
src/omx_brainstorm/
├── models.py          — VideoType, ExpertInsight, MacroInsight, MarketReviewSummary 등
├── pipeline.py        — OMXPipeline (LLM 분석 + VideoType 분기 + 병렬 처리)
├── heuristic_pipeline.py — 휴리스틱 분석 (LLM 없이) + VideoType 분기
├── expert_interview.py — 전문가 인터뷰 인사이트 추출
├── macro_signals.py   — 매크로 시그널/인사이트 추출
├── market_review.py   — 시장리뷰 요약 추출 + MD 렌더링
├── title_taxonomy.py  — VideoType 분류
├── signal_gate.py     — 영상 신호 평가
├── reporting.py       — 개별 리포트 + 통합 대시보드
├── cli.py             — CLI 엔트리포인트
├── ...                — 기타 지원 모듈
```

## 4. 중요 결정 사항과 아키텍처 맥락
- 큰 리팩토링보다 **데이터 먼저 보고 점진 확장** 방향
- 삼프로TV는 "종목 추출"보다 "콘텐츠 유형 구분 + 유형별 인사이트 추출"이 핵심
- transcript fetch는 여전히 IP-block 이슈 → cache + metadata fallback 유지
- 병렬 처리는 ThreadPoolExecutor 사용 (asyncio 불필요, I/O bound 작업에 적합)

## 5. 다음 세션에서 할 수 있는 작업
우선순위 후보:

1. **heuristic_pipeline 결과를 통합 대시보드로 연결**
   - `analyze_video_heuristic()` → dict 기반 결과를 `render_combined_dashboard()` 형식으로 변환
   - 어댑터 또는 heuristic 전용 대시보드 필요

2. **실제 삼프로TV 30일 재실행 + 대시보드 생성**
   - video_type 분기 + expert/macro/market_review 추출이 포함된 상태로 재실행

3. **알림 시스템 연동**
   - notifications.py 기반 Telegram/Discord 알림 연결

4. **LLM 기반 expert claim 강화**
   - 현재 규칙 기반 claim 추출 → LLM으로 더 정교한 주장/근거 추출

5. **llm.py 커버리지 개선 (현재 54%)**
   - 실제 provider 연동 테스트 (mock 기반)

## 6. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`
