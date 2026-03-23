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
- Kim / IT / channel comparison 흐름 구축
- prompt sanitization, provider allowlist, transcript cache path sanitization, safer runtime degradation

### P1: video_type 기반 파이프라인 분기 (완료)
- `pipeline.py`의 `_analyze_resolved_video()`에 VideoType별 분기 로직 구현
  - STOCK_PICK/SECTOR → stock analysis only
  - MARKET_REVIEW → `extract_market_review()` → macro_insights 포함
  - EXPERT_INTERVIEW → `extract_expert_insights()` + `extract_macro_insights()`
  - MACRO/NEWS_EVENT/OTHER → `extract_macro_insights()`
- `heuristic_pipeline.py`의 `analyze_video_heuristic()`에도 동일한 분기 구현
- 에러 복원력: 각 추출기 실패 시에도 리포트 생성 계속
- 테스트: `test_pipeline_branching.py` (13 tests) + `test_heuristic_pipeline.py` (5 tests)

### P2: 전문가 인터뷰 인사이트 구조화 (완료)
- `ExpertInsight` dataclass: expert_name, affiliation, key_claims, topic, sentiment, mentioned_tickers
- `expert_interview.py`: 전문가명/소속 추출 (파이프 구분 패턴 + 일반 패턴), 핵심 주장 추출, 센티먼트 감지, 주제 탐지, 관련 종목 추출
- 삼프로TV 파이프 구분 패턴 (`| 이름 소속 직함`) 지원
- 테스트: `test_pipeline_branching.py` 내 전문가 관련 7개 테스트

### P3: 종목 + 매크로 시그널 결합 대시보드 (완료)
- `reporting.py`의 `render_combined_dashboard()`: 매크로 요약, 시장 리뷰, 전문가 인사이트, 종목 분석 요약, 거장 한줄평, 영상 목록 통합
- 매크로 인사이트 중복 제거 (indicator별 최고 confidence 유지)
- 종목 중복 제거 (ticker별 최고 score 유지)
- `save_combined_dashboard()` → channel 분석 시 자동 생성
- 테스트: `test_combined_dashboard.py` (10 tests)

### 기타 완료
- 모든 소스/테스트/스크립트/config 파일 git 커밋 완료
- `AGENTS.md`, `OMX_AGENT_NOTES.md` 커밋 완료

## 2. 현재 검증 상태
- pytest: **184 passed**
- coverage: **86%**
- 전체 20개 커밋 (세션 1~4 기존 15 + 이번 세션 5)

## 3. 주요 파일 구조
```
src/omx_brainstorm/
├── models.py          — VideoType, ExpertInsight, MacroInsight, MarketReviewSummary 등
├── pipeline.py        — OMXPipeline (LLM 기반 분석 + VideoType 분기)
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
- VideoType enum은 models.py에, 분류 로직은 title_taxonomy.py에, 매크로는 macro_signals.py에, 시장리뷰는 market_review.py에 배치
- transcript fetch는 여전히 IP-block 이슈 → cache + metadata fallback 유지
- comparison / runner / config / scheduler / logging / healthcheck 운영 가능 상태

## 5. 다음 세션에서 할 수 있는 작업
우선순위 후보:

1. **시장리뷰 요약 자동 저장 스크립트**
   - MARKET_REVIEW 유형 영상을 모아 시장리뷰 MD 파일 자동 생성/아카이빙

2. **heuristic_pipeline 결과를 통합 대시보드로 연결**
   - 현재 `analyze_video_heuristic()` 결과는 dict 기반 → `render_combined_dashboard()`는 `VideoAnalysisReport` 기반
   - 변환 어댑터 또는 heuristic 전용 대시보드 필요

3. **실제 삼프로TV 30일 재실행 + 대시보드 생성**
   - video_type 분기 + expert/macro/market_review 추출이 포함된 상태로 재실행
   - 결과 대시보드 리뷰

4. **알림 시스템 연동**
   - notifications.py 기반 Telegram/Discord 알림 연결

5. **LLM 기반 expert claim 강화**
   - 현재 규칙 기반 claim 추출 → LLM으로 더 정교한 주장/근거 추출

## 6. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`
