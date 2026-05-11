# OMX Session Handoff

## Resume rule
다음 세션에서는 `AGENTS.md`와 `SESSION_HANDOFF.md`를 먼저 읽고 이어서 진행한다.

## Workspace snapshot
- Workspace: `/home/wdsr88/workspace/y2i`
- Generated at: `2026-05-11T00:00:00+00:00`
- Branch: `feat/analyst-consensus-plain-summary`
- Latest completed slice: **Reddit English sentiment source pilot**
- Verification:
  - `.venv/bin/python -m pytest tests/test_reddit_source.py tests/test_kindshot_reddit_integration.py tests/test_reddit_signal_freshness.py -q` -> 19 passed
  - `.venv/bin/python -m pytest tests/test_twitter_source.py tests/test_talkboard_source.py tests/test_kindshot_twitter_integration.py tests/test_kindshot_talkboard_integration.py tests/test_kindshot_reddit_integration.py tests/test_reddit_source.py tests/test_reddit_signal_freshness.py -q` -> 63 passed
  - `.venv/bin/python -m omx_brainstorm.cli --help | rg 'ingest-reddit|reddit-healthcheck|ingest-talkboard|ingest-twitter|ingest-news'` -> Reddit commands exposed alongside existing source commands

## Current progress — y2i 5번째 소스: Reddit 영어 sentiment
Existing YouTube, News RSS, Twitter/X, and Korean talkboard sources now have a separate fifth source class:
**old.reddit.com subreddit listing JSON**.

### API option analysis
| Option | Decision | Rationale |
|---|---|---|
| old.reddit.com `.json` listing | selected | Free, no API key, stdlib `urllib/json`, enough for a current-signal pilot. |
| PRAW / OAuth | deferred | More official API semantics, but adds dependency and committed-credential risk if mishandled. |
| Pushshift archive | deferred | Better suited for historical research; availability/terms are separate from live Reddit ingestion. |

### Subreddit curation
| Subreddit | Tier | Note |
|---|---|---|
| r/stocks | high_signal | Broad equity discussion with moderate noise. |
| r/investing | high_signal | Longer-horizon equity/macro discussion. |
| r/SecurityAnalysis | high_signal | Lower volume, deeper fundamental writeups. |
| r/StockMarket | medium | Market-wide, ticker-gated. |
| r/wallstreetbets | noise_filter | High velocity but noisy; requires explicit ticker + sentiment anchors. |
| r/options | noise_filter | Options flow can lead but needs strict filters. |

### Implemented
- `src/omx_brainstorm/reddit_source/`
  - stdlib-only `old.reddit.com/r/<sub>/hot.json?limit=<n>` fetcher
  - visible user-agent, low default inter-subreddit delay, 429/403 surfaced through health state
  - `RedditResolver`, `RedditPost`, JSON parser, classifier, ticker extraction, tracker ingestion, health writer
  - `REDDIT_API_OPTION_ANALYSIS` and `SUBREDDIT_CURATION` constants for reportable pilot rationale
  - `channel_slug="reddit:<subreddit>"`
- Ticker extraction:
  - cashtags: `$SMCI`, `$MU`, `$AVGO`, `$005930`
  - simple regex token NER for known uppercase tickers and `COMPANY_MAP` aliases
  - no LLM calls
- Sentiment:
  - positive keywords such as `moon`, `bullish`, `buy`, `calls`, `long`
  - negative keywords such as `bearish`, `sell`, `short`, `puts`, `crash`
  - outputs `sentiment`, `positive_hits`, `negative_hits`, `signal_score`, `video_signal_class`, `should_analyze_stocks`, `skip_reason`
- `src/omx_brainstorm/cli.py`
  - `ingest-reddit`
  - `reddit-healthcheck`
- `src/omx_brainstorm/kindshot_feed.py`
  - `reddit:` -> `signal_source="y2i:reddit"`
- `scripts/reddit_signal_freshness.py`
  - unique Reddit ticker count, overlap and lead-time vs talkboard/Twitter/news/YouTube, 5d directional hit-rate
- Tests and fixture:
  - `tests/test_reddit_source.py`
  - `tests/test_kindshot_reddit_integration.py`
  - `tests/test_reddit_signal_freshness.py`
  - `tests/fixtures/reddit_listing_wallstreetbets.json`

## Important decisions
- Reddit English sentiment is mainly for US alpha-scanner names now; KR usefulness is limited until KR universe matching expands.
- No Reddit API keys, OAuth credentials, PRAW dependency, browser automation, or Pushshift dependency.
- `wallstreetbets` and `options` are treated as noise-filter tiers, not automatically high-confidence sources.
- The source writes US and KR tickers to the signal tracker; Kindshot export still applies its own KR export gate.
- Existing YouTube/news/twitter/talkboard ingestion behavior was kept additive-only.

## Blockers / limitations
- Reddit ToS/rate limits remain an operator responsibility; production jobs should keep low request volume and a clear user-agent.
- old.reddit.com listing JSON is current-window ingestion, not historical archive search.
- Rule sentiment is intentionally simple and noisy; run a pilot before assigning strong channel weights.
- Workspace contains unrelated pre-existing dirty/untracked files (`DASHBOARD.md`, `.memsearch/`, `.omg/`, `dashboard/app_fixed.py`, `ralph-loop.state.json*`) that are not part of the Reddit slice.

## Exact next step
Run the 14-day Reddit pilot, then:
```bash
python -m scripts.reddit_signal_freshness \
  --tracker-db .omx/state/signal_tracker.json \
  --window-days 14 \
  --output .omx/research/reddit-pilot-14day-review.json
```

Use `reddit_tickers_unique`, `lead_time_summary`, and `hit_rate_5d.reddit` to decide whether `reddit:` should receive normal, discounted, or watch-only weighting for US alpha-scanner and future KR universe matching.
