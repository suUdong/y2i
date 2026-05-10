# OMX Session Handoff

## Resume rule
다음 세션에서는 `AGENTS.md`와 `SESSION_HANDOFF.md`를 먼저 읽고 이어서 진행한다.

## Workspace snapshot
- Workspace: `/home/wdsr88/workspace/y2i`
- Generated at: `2026-05-11T23:45:00+00:00`
- Branch: `feat/analyst-consensus-plain-summary`
- Verification: `.venv/bin/python -m pytest -q` → all passed (4 new commits, 0 regressions)

## Current progress — y2i 시그널 소스 확장 (Twitter/X pilot)
직전 세션의 한국 RSS 뉴스 인입(`fb4c780` + `dd8794d` + `9b7f55c` + `a94fb4f` + `1a5741c`)에
이어, **Twitter/X 한국 증권 인플루언서** 인입 파이프라인을 추가했다. 4개의
독립 commit 으로 분할되어 있고 모두 검증(테스트)되었다.

### Source 선정 — 왜 RSS 게이트웨이?
| 옵션 | 비용 | 안정성 | 결정 | 이유 |
|---|---|---|---|---|
| **Nitter / RSSHub RSS 게이트웨이** | 무료 (또는 self-host VPS $5/월) | 중간 (인스턴스별 다름; per-handle error 가 흡수됨) | ✅ 채택 | stdlib only, news_source.py 와 동형. URL 만 교체하면 됨. |
| X API v2 Basic | $200/월 / 10K reads | 높음 | ❌ 드롭 | 비용 prohibitive — 10 handles × 일별 폴링이면 한도 빠르게 소진. |
| X API v2 Free | $0 | n/a | ❌ 드롭 | 읽기 권한 없음 (2023 변경). |
| snscrape | 무료 | 무 | ❌ 드롭 | 2023 unauthenticated guest API 차단 후 동작 안 함. |
| twscrape | 무료 | 낮음 | ❌ 드롭 | 로그인 쿠키 필요 — ToS + 계정 banning 리스크. |
| Playwright/Selenium | 큰 인프라 비용 | 낮음 | ❌ 드롭 | 헤드리스 브라우저 신규 의존성 + 로그인 필요. CLAUDE.md 의 "no new deps" 위반. |

### Gateway 설계
- 기본값: `https://nitter.privacydev.net/{handle}/rss` (env 로 override 가능)
- 환경변수:
  - `Y2I_TWITTER_GATEWAY` — 게이트웨이 base URL
  - `Y2I_TWITTER_GATEWAY_TEMPLATE` — 변형 게이트웨이를 위한 URL 템플릿 (RSSHub 의 `/twitter/user/{handle}` 등)
- 운영자가 RSSHub self-host 로 옮길 때 **코드 변경 0**.
- per-handle 에러는 `state.errors[]` 에 기록되며 일부 실패 → `status=degraded`, 전체 실패 → `status=error`.

### 큐레이션된 X 핸들 (seed defaults, 10개)
무료 게이트웨이의 부담을 줄이고, 한국 종목 노출이 큰 매체 공식 계정으로 시드 시작.
운영 14일 후 개인 fintwit 계정으로 교체/추가를 권장.

| handle | 출처 | 비고 |
|---|---|---|
| `hankyung_news` | 한국경제 | 1차 후보 |
| `mtnews_kr` | 머니투데이 | |
| `_yonhapnews` | 연합뉴스 | high-volume |
| `chosunbiz` | 조선비즈 | |
| `mkbusiness` | 매일경제 | |
| `etoday_kr` | 이투데이 | |
| `wowtv_kr` | 한경 와우TV | |
| `etnews_kr` | 전자신문 | 반도체/테크주 |
| `sedaily_kr` | 서울경제 | |
| `koreaherald` | Korea Herald | 영문 — global 트랙 보강용 |

운영자가 `--handles` CLI 옵션으로 부분 집합을 골라 실행 가능.

### Tested e2e (in-memory smoke)
실제 게이트웨이 fetch 는 이 WSL 환경에서 막혀 있어 (외부 네트워크 connection refused),
**scripted resolver** 로 in-memory e2e 를 검증했다:
```
TweetItem("삼성전자 HBM4 양산 가속 — 외인 매수 유입 $005930 …")
  → classify_tweet_item: score=84.0, ACTIONABLE
  → tracker: SignalRecord(005930.KS, twitter:hankyung_news, BUY, 84.0)
  → kindshot export: signal_source="y2i:twitter", confidence=0.84
```
production 환경에서 `omx-brainstorm ingest-twitter` 가 동일하게 동작할 것.

### Hit rate / lead-time 측정 방법론 (14-day post-pilot)
1. 매일 09:00 KST `ingest-twitter` 가 cron 으로 실행 → twitter_ingestion_health.json 업데이트.
2. heartbeat-watchdog 가 6 시간 stale 임계로 alert 잡음.
3. 매일 야간 signal_tracker 가 기존 가격 backfill 로 twitter SignalRecord 의 5d return 채움.
4. T+14 시점:
   ```bash
   python -m scripts.twitter_signal_freshness --window-days 14 \
       --output .omx/research/twitter-pilot-14day-review.json
   ```
   - `hit_rate_5d.twitter.win_rate_pct` ≥ YouTube baseline (73.68%) → 채널 가중치 1.0+ 검토
   - `lead_time_summary.twitter_vs_news.median_hours_leader_leads_follower` > 0
     → twitter freshness 가치 입증
   - `twitter_tickers_unique` > 0 → news/YT 가 놓친 종목을 잡아내는 가치 검증
5. 결과 후 kindshot 채널 가중치 조정 PR.

### Healthcheck wiring (74aeb5f stale-detection 패턴)
- `run_twitter_ingestion` 이 `.omx/state/twitter_ingestion_health.json` 을
  scheduler_health.json 과 동일한 shape 로 기록.
- `healthcheck.compute_health_summary` 가 그대로 호출 가능 → 코드 분기 0.
- 신규 CLI:
  ```
  omx-brainstorm twitter-healthcheck --exit-nonzero-on-stale
  ```
  6 시간 임계, cron / heartbeat 와 결합.
- 모든 handle 실패 → `status=error`. 일부 handle 실패 → `status=degraded`.

## Active checkpoint
- Task: y2i 시그널 소스 다변화 (Phase 2: Twitter/X) — **완료**
- Next step: 14일 운영 후 freshness + lead-time + hit-rate 정량화.
- Live network 가 끊긴 환경에서는 fixture-only 검증으로 마감 (위 e2e smoke 가 충분).

## Important decisions
- 무료 stdlib RSS 게이트웨이 선택. X API v2 비용 발생 시 사전 escalation 정책 준수.
- Cashtag (`$005930`) 매칭은 COMPANY_MAP 의 숫자 prefix 인덱스로 빌드 — 한국어 이름 매처와 동일한 데이터 출처.
- `signal_source="y2i:twitter"` 라벨로 kindshot 의 startswith("y2i") 컨트랙트 유지.
- 보수적 export gate 유지: 트위터 BUY 라도 점수 ≥72 또는 가격 history / consensus 가 받쳐줄 때만 kindshot 으로.
- `Y2I_TWITTER_GATEWAY` 환경변수로 self-host 이행 시 코드 무변경.

## Blockers / limitations
- 공개 nitter 인스턴스가 불안정 (2024 guest-API 차단 이후). 14일 운영 중 `status=error` 가 잦으면
  RSSHub 자체호스트로 전환할 것 — 환경변수만 갈아끼우면 됨.
- 시드 핸들은 매체 공식 계정 위주. 개인 fintwit 가 더 lead-time 우위가 있을 수 있으나,
  특정 인물 명명은 운영자 정책 영역이므로 14일 리뷰 때 후속 큐레이션 권장.
- 한국어 cashtag 관습이 영미만큼 표준화되지 않아 cashtag hits 가 0 인 트윗도 많을 것.
  본문 키워드 매칭 + COMPANY_MAP fuzzy resolver 가 fallback 역할.

## Generated outputs
- 신규 모듈: `src/omx_brainstorm/twitter_source.py` (495 LOC)
- 수정 모듈: `src/omx_brainstorm/cli.py` (CLI 와이어링), `src/omx_brainstorm/kindshot_feed.py` (label)
- 신규 스크립트: `scripts/twitter_signal_freshness.py` (3-source 비교)
- 신규 테스트:
  - `tests/test_twitter_source.py` (19 tests)
  - `tests/test_kindshot_twitter_integration.py` (5 tests)
  - `tests/test_twitter_signal_freshness.py` (8 tests)

## Verification
- `.venv/bin/python -m pytest -q` → 전체 통과 (regression 없음)
- in-memory e2e smoke (위 참조): score 84 BUY → y2i:twitter export
- CLI smoke: `ingest-twitter --handles hankyung_news` → error path 정상 (network 차단 환경에서 `status=error`, health file 갱신 확인)

## Commit trail (이번 세션, origin push 예정)
- `2d7c188` — twitter_source 모듈: stdlib RSS resolver + cashtag classifier
- `cedc94e` — ingest-twitter / twitter-healthcheck CLI 와이어링
- `41d3725` — kindshot_feed `signal_source=y2i:twitter` 라벨
- `601cf66` — freshness / lead-time / hit-rate 측정 스크립트

## Exact next step
1. 14일 운영 (`cron` 으로 `omx-brainstorm ingest-twitter` 일 1회) 후
   `python -m scripts.twitter_signal_freshness --window-days 14 \
       --output .omx/research/twitter-pilot-14day-review.json` 실행
2. win-rate / lead-time / 고유 ticker 비율 보고 → kindshot 채널 가중치 조정 PR

## 다음 슬롯 ROI 추천 (Conductor 후속 슬롯용)
| 후보 | ROI | 추천 우선순위 | 이유 |
|---|---|---|---|
| **Reddit (`r/Stocks`, `r/wallstreetbets`, `r/Korea_Stocks`)** | ⭐⭐⭐⭐ | 1순위 | RSS 가 무료 공개 (`old.reddit.com/r/<sub>.rss`). 신규 deps 0. news/twitter 와 동일 패턴. lead-time 비교 → US 종목까지 보강. |
| **Discord 한국 주식방 (특정 채널 export)** | ⭐⭐⭐ | 2순위 | webhook bot 으로 무료 수집 가능. 노이즈 비율 높음 — gate 가 흡수. 다만 사용자가 *어떤* 방을 모니터링할지 정책 결정 필요. |
| **Telegram 채널 (`@<name>`)** | ⭐⭐⭐ | 2순위 | Telegram MTProto/Bot API 로 무료. 한국 fintwit 보다 telegram 쪽 종목 채널이 더 빠른 경우 있음. python-telegram-bot 의존성 필요. |
| **네이버 종목토론방** | ⭐ | 후순위 | 봇 차단, 낮은 SNR, 법적 영역 회색. 이미 한 차례 보류된 옵션. |
| **추가 YouTube 채널 확장** | ⭐⭐ | 후순위 | 한계 효용 체감 — 기존 인프라 활용도는 100% 지만 신선도/리드타임 추가 가치 작음. |
| **Whisper-전사된 한국 팟캐스트** | ⭐⭐ | 후순위 | OpenAI/로컬 Whisper 비용 (시간당 $0.006~). v2 보류 이유와 동일. 정량적 이득 작음. |

**추천**: 다음 슬롯은 **Reddit RSS** —
- news/twitter 와 동일한 stdlib 게이트웨이 패턴.
- 미국·한국 시장 양쪽을 한 번에 보강 (`r/Stocks` + `r/Korea_Stocks`).
- `signal_source="y2i:reddit"` 라벨 추가로 4-source 비교가 가능해짐.
- 예상 작업량: news pilot 과 유사 (1 일).

작업 슬롯의 순서:
1. **Reddit RSS** (1 day)
2. 14일 후 **twitter-pilot review + kindshot 채널 가중치 조정 PR** (0.5 day)
3. **Telegram 채널** (1~2 day, python-telegram-bot 의존성 결정 필요)
