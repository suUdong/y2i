# OMX Session Handoff

## Resume rule
다음 세션에서는 `AGENTS.md`와 `SESSION_HANDOFF.md`를 먼저 읽고 이어서 진행한다.

## Workspace snapshot
- Workspace: `/home/wdsr88/workspace/y2i`
- Generated at: `2026-05-11T00:00:00+00:00`
- Branch: `feat/analyst-consensus-plain-summary`
- Latest completed slice: **Korean talkboard source pilot**
- Verification:
  - `.venv/bin/python -m pytest tests/test_talkboard_source.py tests/test_kindshot_talkboard_integration.py tests/test_talkboard_signal_freshness.py -q` -> 25 passed
  - `.venv/bin/python -m pytest -q` -> full suite passed
  - `.venv/bin/python -m omx_brainstorm.cli --help | rg 'talkboard|ingest-news|ingest-twitter'` -> `ingest-talkboard` and `talkboard-healthcheck` exposed
  - Architect verification -> APPROVED

## Current progress — y2i 4번째 소스: 한국 종목 토론방
Twitter/X and News RSS patterns were extended with a fourth source class:
**Naver Finance first-page 종목토론방**.

### Source 선정
| Source | Decision | Rationale |
|---|---|---|
| Naver Finance talkboard | selected | Public first-page HTML, no API key, high KR retail signal density, stdlib parsable. |
| Toss Securities community | deferred | Public route exists but useful comments are client/app/API surfaced; not a stable free stdlib ingestion target. |
| Stockplus/KakaoStock | deferred | Stockplus public pages are SPA/API oriented and `/api` is robots-disallowed; KakaoStock legacy domain is not a usable public board target. |

Naver robots/crawl boundary: use only
`https://finance.naver.com/item/board.naver?code=<6 digits>`.
The implementation rejects non-Naver URLs, non-`/item/board.naver` paths, and any `page` query key including blank `page=`.
No `board_read` body crawling and no pagination.

### Implemented
- `src/omx_brainstorm/talkboard_source.py`
  - stdlib-only HTTP + regex/HTML parsing
  - `TalkboardResolver`, `TalkboardPost`, parser, classifier, ticker extraction, tracker ingestion, health writer
  - rule-based sentiment only: positive keywords (`매수`, `상승`, `돌파`, etc.) and negative keywords (`매도`, `하락`, `손절`, etc.)
  - `channel_slug="talkboard:naver:<code>"`
- `src/omx_brainstorm/cli.py`
  - `ingest-talkboard`
  - `talkboard-healthcheck`
- `src/omx_brainstorm/kindshot_feed.py`
  - `talkboard:` -> `signal_source="y2i:talkboard"`
- `scripts/talkboard_signal_freshness.py`
  - freshness, unique ticker count, lead-time vs Twitter/News/YouTube, 5d directional hit-rate
  - lead-time uses first signal **inside the requested window**
- Tests:
  - `tests/test_talkboard_source.py`
  - `tests/test_kindshot_talkboard_integration.py`
  - `tests/test_talkboard_signal_freshness.py`

## Important decisions
- No paid APIs, no API keys, no browser automation, no login/session scraping.
- Live fetcher keeps a visible user agent and default inter-board delay.
- URL validation blocks pagination to respect the narrow robots-compatible surface.
- Sentiment stays deterministic/rule-based because LLM credits are constrained.
- Kindshot integration remains prefix-based, matching `news:` and `twitter:` patterns.

## Blockers / limitations
- Robots.txt is not permission. Production use should still confirm service terms and run at conservative rates.
- Naver may change HTML markup; fixture tests cover the current expected row structure, and parse/fetch failures surface through health state.
- Talkboard sentiment is noisy by design; the first 14-day pilot should decide source weighting from measured hit-rate/lead-time.
- Workspace still contains unrelated dirty/untracked files from before/alongside this task (`DASHBOARD.md`, `src/omx_brainstorm/reddit_source.py`, Reddit CLI hunks in `cli.py`, `.memsearch/`, `.omg/`, dashboard scratch files). These were intentionally not claimed for this slice.

## Exact next step
Run the 14-day pilot, then:
```bash
python -m scripts.talkboard_signal_freshness \
  --tracker-db .omx/state/signal_tracker.json \
  --window-days 14 \
  --output .omx/research/talkboard-pilot-14day-review.json
```

Use `talkboard_tickers_unique`, `lead_time_summary`, and `hit_rate_5d.talkboard` to decide whether `talkboard:` should receive normal, discounted, or watch-only kindshot weighting.
