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
- 기본 YouTube → transcript/metadata → signal gate → ticker extraction → fundamentals → master opinions → ranking → backtest 파이프라인 구축
- transcript cache 영속화 (`.omx/cache/transcripts/`)
- metadata fallback, scheduler, healthcheck, JSON logging, Docker, config 지원 추가
- Kim / IT / channel comparison 흐름 구축
- 대규모 테스트/커버리지 확장:
  - 현재 pytest: `121 passed`
- 구조 리팩터링 진행:
  - `stock_registry.py`
  - `heuristic_pipeline.py`
  - `evaluation.py`
  - `comparison.py`
  - `transcript_runtime.py`
  - `errors.py`
- prompt sanitization, provider allowlist, transcript cache path sanitization, safer runtime degradation 등 주요 보안/안정화 작업 반영

## 2. 삼프로TV 관련 완료된 것

### 채널 연동
- `config.toml`에 삼프로TV 추가 완료
- `config.example.toml`에 삼프로TV 추가 완료
- `channels.json`에 삼프로TV 추가 완료
- channel id: `UChlv4GSd7OQl3js-jkLOnFA`
- url: `https://www.youtube.com/@3protv/videos`

### 제목 수집
- 최대 3년치 title inventory 수집 완료 (1200개)

### 영상 유형 체계 확정 (이번 세션 완료)
- `VideoType` enum 7개 값 코드화 완료:
  - `STOCK_PICK`, `MACRO`, `MARKET_REVIEW`, `EXPERT_INTERVIEW`, `SECTOR`, `NEWS_EVENT`, `OTHER`
- `classify_video_type(title, description, tags)` 함수 구현
- `VideoSignalAssessment`에 `video_type` 필드 추가
- `assess_video_signal()`에서 자동 분류 통합
- priority-ordered rules + description/tags fallback 구조
- 15개 테스트 통과

### 매크로 인사이트 추출 파이프라인 (이번 세션 완료)
- `MacroInsight` dataclass 정의 (indicator, direction, label, confidence, sentiment, beneficiary_sectors)
- `extract_macro_insights(title, text)` 함수 구현
- 7개 지표 커버:
  - `interest_rate` (금리)
  - `fx` (환율/달러)
  - `oil` (유가)
  - `fomc` (FOMC/연준)
  - `cpi` (물가/CPI)
  - `employment` (고용/실업)
  - `sector_rotation` (섹터 로테이션)
- 각 지표별 UP/DOWN/NEUTRAL 방향성 + sentiment + confidence 추출
- 12개 테스트 통과

### 시장리뷰 전용 요약 기능 (이번 세션 완료)
- `MarketReviewSummary` dataclass 정의 (indices, direction, risk_events, sector_focus, key_points, macro_insights)
- `extract_market_review(title, text)` 함수 구현:
  - 6개 지수 패턴 (코스피/코스닥/나스닥/S&P500/다우/니케이)
  - 전체 시장 방향성 (BULLISH/BEARISH/NEUTRAL)
  - 19개 리스크 이벤트 키워드
  - 18개 섹터 키워드 → 정규화된 섹터 레이블
  - 핵심 포인트 자동 추출
  - 매크로 인사이트 통합
- `render_market_review_md(summary)` → 구조화된 마크다운 생성
- 14개 테스트 통과

### 기존 파이프라인 30일 실행
- 현재 종목 중심 heuristic 파이프라인 그대로 삼프로TV 30일 실행 완료
- 분석 영상 수: `80` / ACTIONABLE: `37` / NOISE: `43`

## 3. 삼프로TV 관련 남은 것
- 필요시 이후:
  - 전문가 인터뷰형 인사이트 구조화
  - 종목 시그널 + 매크로 시그널 결합 대시보드
  - video_type별 파이프라인 분기 (MACRO → extract_macro_insights, MARKET_REVIEW → extract_market_review)
  - 시장리뷰 요약 자동 저장 스크립트
  - heuristic_pipeline에 video_type 활용한 분석 경로 최적화

## 4. 다음 세션에서 해야 할 작업
우선순위 후보:

1. **video_type 기반 파이프라인 분기**
   - MACRO 유형 → extract_macro_insights로 라우팅
   - MARKET_REVIEW 유형 → extract_market_review + render_market_review_md로 라우팅
   - heuristic_pipeline.py 또는 pipeline.py에 분기 로직 추가

2. **전문가 인터뷰형 인사이트 구조화**
   - EXPERT_INTERVIEW 유형에서 전문가명, 소속, 핵심 주장 추출

3. **종목 + 매크로 시그널 결합 대시보드**
   - 종목 시그널과 매크로 시그널을 하나의 리포트로 통합

## 5. 중요 결정 사항과 아키텍처 맥락
- 큰 리팩토링보다 **데이터 먼저 보고 점진 확장** 방향으로 전환함
- 삼프로TV는 종목 추천 채널로 단순 취급하지 않기로 함
- 삼프로TV는 "종목 추출"보다 "콘텐츠 유형 구분 + 유형별 인사이트 추출"이 더 중요하다는 판단
- VideoType enum은 models.py에, 분류 로직은 title_taxonomy.py에, 매크로 인사이트는 macro_signals.py에, 시장리뷰는 market_review.py에 배치
- transcript fetch는 여전히 IP-block 이슈가 있으므로 cache + metadata fallback 유지
- 현재 comparison / runner / config / scheduler / logging / healthcheck 는 이미 운영 가능 상태

## 6. 생성된 주요 파일 목록
- `config.toml` / `config.example.toml` / `channels.json`
- `src/omx_brainstorm/title_taxonomy.py` — VideoType 분류 (classify_video_type + legacy classify_title)
- `src/omx_brainstorm/macro_signals.py` — 매크로 시그널 + 매크로 인사이트 추출
- `src/omx_brainstorm/market_review.py` — 시장리뷰 요약 추출 + MD 렌더링
- `src/omx_brainstorm/models.py` — VideoType enum, MacroInsight, MarketReviewSummary dataclasses
- `tests/test_video_type_classification.py` — 15 tests
- `tests/test_macro_insights.py` — 12 tests
- `tests/test_market_review.py` — 14 tests
- `scripts/collect_channel_titles.py` / `scripts/analyze_title_inventory.py` / `scripts/run_channel_30d_single.py`

## 7. 관련 커밋 목록
- `e88e4a4` Remove unsafe Codex bypass flag from default provider
- `c1d8e8a` Validate custom provider commands against a binary allowlist
- `cc02dd3` Ignore local config, runtime state, and build artifacts
- `ea7065c` Sanitize user-controlled prompt content before interpolation
- `8c5cc82` Log transcript fetch fallback before degrading to cache or metadata
- `c66d795` Add SamproTV as a data source and capture title inventory
- `53efc58` Codify video type taxonomy with VideoType enum and classification
- `ddb84c2` Add structured macro insight extraction pipeline
- `1377c1a` Add market review summary extraction and markdown rendering
- `7982b00` Remove unreachable hybrid branch in classify_video_type

## 8. 현재 검증 상태
- pytest: `121 passed`
- 3개 user story 전체 architect 검증 PASS
- 기존 80개 테스트 + 신규 41개 테스트 = 121개 전체 통과

## 9. 다음 세션 시작 체크리스트
다음 세션에서 가장 먼저 읽을 것:
- `AGENTS.md`
- `SESSION_HANDOFF.md`

그 다음 바로 할 것:
1. video_type 기반 파이프라인 분기
2. 전문가 인터뷰 인사이트 구조화
3. 종목 + 매크로 결합 대시보드
