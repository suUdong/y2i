# Claude Workspace Guide

## First Read
- Read `AGENTS.md` first.
- In a resumed session, read `SESSION_HANDOFF.md` immediately after `AGENTS.md`.
- Read `~/workspace/WORKSPACE.md` for the cross-project map.
- Search memsearch for prior context before re-discovering old decisions.
- Check `git status --short` before editing. The worktree may already contain user changes.

## Workspace Context
y2i is a Tier 2 signal/intelligence project in the shared workspace.

- Role: turn YouTube videos into investable signals
- Upstream context: macro-intelligence provides regime context
- Downstream consumers: kindshot and alpha-scanner
- Human-facing surface: dashboards are the final interface; agents are expected to read and modify code directly

Data flow:
`[YouTube] -> y2i -> [signal feed] -> kindshot, alpha-scanner`

## Default Operating Mode
- Operate in `ralph` mode by default.
- Complete bounded slices end-to-end: brainstorming, design, development, review, testing, final artifact.
- Do not stop for intermediate approval unless there is a real blocker.
- A real blocker means missing external access, unavailable required data, broken credentials, or a hard technical constraint that cannot be worked around safely.

## Repository Goal
Analyze economic and macro YouTube channels end-to-end:
1. collect channel videos
2. fetch transcripts and metadata
3. run a video usefulness and signal gate
4. extract tickers only from videos worth analyzing
5. fetch current basic financial snapshots
6. generate master-style one-line opinions
7. produce text-based reports, rankings, and backtests

## Required Video Usefulness Gate
Every video must be classified before stock analysis.

Allowed classes:
- `ACTIONABLE`
- `SECTOR_ONLY`
- `LOW_SIGNAL`
- `NOISE`
- `NON_EQUITY`

Required fields:
- `signal_score`
- `video_signal_class`
- `should_analyze_stocks`
- `skip_reason`

## Analysis Output Requirements
For each selected stock, final text output should include:
- current basic financial state
- key basic indicators summary
- basic verdict
- master-by-master one-line verdicts
- final aggregate verdict
- invalidation triggers

## Master Framework
- Keep the architecture extensible; Druckenmiller is only the first master, not the only one.
- Current initial masters:
  - `druckenmiller`
  - `buffett`
  - `soros`

## Shared Infrastructure
- `~/workspace/WORKSPACE.md`: top-level workspace map
- `memsearch`: search previous session memory and debugging history
- `agent-deck`: session orchestration layer
- `README.md`: repository-local CLI, config, scheduler, and backtest details

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev,dashboard]
```

## Verification
```bash
pytest
pytest --cov=src/omx_brainstorm --cov-report=term-missing
```

## Useful Commands
```bash
omx-brainstorm register-channel "https://www.youtube.com/@lucky_tv/videos"
omx-brainstorm list-channels
omx-brainstorm analyze-video "https://www.youtube.com/watch?v=..."
omx-brainstorm analyze-channel "https://www.youtube.com/@lucky_tv/videos" --limit 5
omx-brainstorm run-comparison --config config.toml
omx-brainstorm signal-backtest-report --config config.toml --lookback-days 90
omx-brainstorm run-scheduler --config config.toml --once
omx-brainstorm run-healthcheck
```

## Editing Rules
- Keep diffs small, reviewable, and reversible.
- Prefer reusing existing utilities and patterns over adding new abstractions.
- Prefer deletion over addition when simplifying.
- Do not add new dependencies unless explicitly requested.
- Do not overwrite unrelated user changes.
- Verify before claiming completion.
