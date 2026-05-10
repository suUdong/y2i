# OMX Session Handoff

## Resume rule
다음 세션에서는 `AGENTS.md`와 `SESSION_HANDOFF.md`를 먼저 읽고 이어서 진행한다.

## Workspace snapshot
- Workspace: `/home/wdsr88/workspace/y2i`
- Generated at: `2026-05-11T23:30:00+00:00`
- Branch: `feat/analyst-consensus-plain-summary`
- HEAD: `a94fb4f` (US-001..US-005 모두 origin 에 push 완료)
- Verification: `pytest -q` 558 passed · backtest `73.68%` win rate (regression 없음)

## Current progress — y2i 시그널 소스 확장 (News RSS pilot)
직전 fire-w11-y2i-ks 의 파이프라인 견고화(74aeb5f) 위에, y2i 입력 다변화의 첫 단계로
**한국 증권 뉴스 RSS** 인입 파일럿을 완성했다. 5개 user story(US-001..US-005) 가
모두 통과했고 각 단계가 단일 커밋으로 origin 에 push 되어 있다.

### Source 선정 — 왜 한국 증권 뉴스 RSS?
| 후보 | ROI | 채택 여부 | 이유 |
|---|---|---|---|
| **Korean financial news RSS** (한경/머투/이데일리) | ⭐⭐⭐⭐⭐ | ✅ 1순위 | 구조화·실시간·무료. 기존 `signal_gate` 키워드와 `resolve_kr_ticker` 재사용. 새 의존성 0. |
| Korean stock podcasts (슈카월드, 김작가TV) | ⭐⭐ | 보류 (v2) | Whisper 전사 비용 (시간당 $). |
| 종목토론방 (네이버 금융 / 토스) | ⭐ | 보류 (v2) | 봇 차단 + 낮은 신호 대 잡음. |
| Twitter/X 한국 증권 인플루언서 | ☆ | 드롭 | API 비용 prohibitive ($100/월~). |

### 채택한 소스 (live 검증 완료)
| outlet | URL | 상태 |
|---|---|---|
| `hankyung` | `https://www.hankyung.com/feed/finance` | ✅ 200 OK, 10 items/fetch |
| `mt` (머투) | `https://rss.mt.co.kr/mt_news.xml` | ✅ 200 OK, 10 items/fetch |
| `edaily` | `https://rss.edaily.co.kr/stock_news.xml` | ⚠️ SSL UNSUPPORTED_PROTOCOL — outlet error 가 health state 에 기록되며 나머지 outlet 은 계속 동작 |

### 첫 ingestion 결과 (smoke run, hankyung + mt, 30 items/outlet)
- **fetched_items**: 60
- **ingested SignalRecords**: 5 (변환률 8.3%)
- **skipped**: 55 (대부분 LOW_SIGNAL — 비종목 헤드라인)
- **dedup 후 unique tickers**: 4 (`005930.KS`, `000660.KS`, `005380.KS`, `006280.KS`)
- **kindshot 으로 export 통과**: 1 (`005930.KS` 삼성전자 BUY, conf 0.93, `signal_source=y2i:news`)

### 시간차 — RSS vs YouTube
- 같은 날 (2026-05-10) 기준 news 4 ticker · YT 0 ticker (현재 production tracker, 7-day window).
- 정량적 lead-time 측정은 `scripts/news_signal_freshness.py` 가 자동 산출 — 14일 운영 데이터 축적 후 의미 있는 lead 값 산출 가능.
- 초기 관찰: RSS 는 **장중·시간외 헤드라인 단위** 라 YouTube (daily upload) 대비 **분~시간 단위 freshness** 확보 가능. 14일 후 실측 예정.

### Hit rate 측정 방법론 (14-day post-pilot 계획)
1. 매일 09:00 KST `ingest-news` 가 cron 으로 실행 (heartbeat-watchdog 가 stale 감지).
2. 매일 야간 `signal_tracker` 가 기존 가격 backfill 로 news SignalRecord 의 5d return 채움.
3. T+14 일 시점에서 `python -m scripts.news_signal_freshness --window-days 14` 실행 →
   - `news_hit_rate_5d.win_rate_pct` ≥ YouTube baseline (5d 73.68%) 이면 채널 가중치 1.0+ 부여 검토.
   - `lead_time_summary.median_hours_news_leads_yt` > 0 이면 freshness 가치 검증됨.
4. 결과를 `.omx/research/news-pilot-14day-review.md` 로 정리 → kindshot 채널 가중치 PR.

### Healthcheck wiring (74aeb5f stale-detection 패턴 적용)
- `run_news_ingestion` 이 `.omx/state/news_ingestion_health.json` 을 scheduler_health.json 과 동일한 shape (last_run_at / last_success_at / last_error_at / status / error_count) 로 기록.
- `healthcheck.compute_health_summary` 가 그대로 호출 가능 → 코드 분기 없음.
- 신규 CLI: `omx-brainstorm news-healthcheck --exit-nonzero-on-stale` (6 시간 임계) 으로 cron / heartbeat 와 결합.
- 모든 outlet 실패 → `status=error`. 일부 outlet 실패 → `status=degraded` 이지만 `last_success_at` 갱신.

## Active checkpoint
- Task: y2i 시그널 소스 다변화 (Phase 1: News RSS)
- Next step: 14일 운영 후 `news_signal_freshness` 리포트로 hit-rate 와 lead-time 정량화 → kindshot 채널 가중치 조정 PR.
- Scope: `src/omx_brainstorm/news_source.py`, `src/omx_brainstorm/cli.py`, `src/omx_brainstorm/kindshot_feed.py`, `scripts/news_signal_freshness.py`, 그리고 4개 test 파일.

## Important decisions
- 의존성 정책 (CLAUDE.md) 준수: `feedparser` 없이 stdlib `urllib + xml.etree.ElementTree` 로 RSS 파싱.
- `channel_slug='news:<outlet>'` 명명 규칙: 기존 SignalTrackerDB / kindshot_feed 가 source-agnostic 이라 코드 분기 0 으로 신규 소스 합류 가능.
- `signal_source` 라벨을 flat `"y2i"` 에서 `"y2i:<source-class>"` (`y2i:youtube` / `y2i:news`) 로 확장 — kindshot 컨슈머는 `startswith("y2i")` 만 보면 됨 (e2e contract test 갱신).
- 보수적 export gate 유지: news BUY 라도 점수 ≥72 일 때만 (또는 신규 7일 내 가격 history / consensus 가 받쳐줄 때) kindshot 으로 흐른다. score 65~71 BUY 는 tracker 에는 들어가지만 export 는 안 됨 → false-positive 흡수.

## Blockers / limitations
- `edaily` RSS 엔드포인트가 구식 SSL protocol 만 지원 → 현재 워크스페이스의 OpenSSL 3.x 정책으로 차단. 다른 outlet (`hankyung`, `mt`) 으로 충분히 cover 되며 health state 가 outlet 단위 error 를 기록한다.
- Fuzzy `resolve_kr_ticker` 가 "현대지에프홀딩스" → 005380.KS 처럼 가끔 mis-match. 점수 65~71 WATCH 로 떨어져 export 단계에서 자연 filter 되지만, 14일 리뷰 때 false-positive 비율 정량 측정 후 prefix matcher 튜닝 후보.

## Generated outputs
- 신규 모듈: `src/omx_brainstorm/news_source.py`
- 수정 모듈: `src/omx_brainstorm/cli.py`, `src/omx_brainstorm/kindshot_feed.py`
- 신규 스크립트: `scripts/news_signal_freshness.py`
- 신규 테스트: `tests/test_news_source.py`, `tests/test_kindshot_news_integration.py`, `tests/test_news_signal_freshness.py`
- 수정 테스트: `tests/test_signal_tracker.py`, `tests/test_end_to_end_kindshot_handoff.py`

## Verification
- `.venv/bin/python -m pytest -q` → 558 passed
- `.venv/bin/python -m scripts.backtest_kindshot_export --fresh` → 5d win rate 73.68% (변화 없음)
- Live smoke: `.venv/bin/python -m omx_brainstorm.cli ingest-news --outlets hankyung,mt` → 5 SignalRecords, kindshot export 통과 1건

## Commit trail (origin/feat/analyst-consensus-plain-summary)
- `fb4c780` — US-001: stdlib RSS resolver + classifier
- `dd8794d` — US-002+US-003: ingest-news / news-healthcheck CLI + stale-detection wiring
- `9b7f55c` — US-005: kindshot_feed signal_source labelling
- `a94fb4f` — US-004: freshness + lead-time + hit-rate script

## Exact next step
- 14일 운영 후 `python -m scripts.news_signal_freshness --window-days 14 --output .omx/research/news-pilot-14day-review.json` 실행, win-rate / lead-time 정량화 → kindshot 채널 가중치 조정 PR 작성.
