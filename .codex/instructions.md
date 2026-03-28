# Y2I - Codex Instructions

See `AGENTS.md` first, then `~/workspace/WORKSPACE.md`.

## Role In Workspace
- Tier 2 signal/intelligence project
- Converts YouTube content into investment signals
- Feeds kindshot and alpha-scanner
- Uses macro-intelligence as upstream context when regime matters
- Dashboards are the final human-facing surface; agents should inspect and modify code directly

Data flow:
`[YouTube] -> y2i -> [signal feed] -> kindshot, alpha-scanner`

## Default Execution
- Run in `ralph` mode by default.
- Finish bounded slices end-to-end without stopping for routine approval.
- Ask only when blocked by missing access, missing data, broken credentials, or a destructive fork in the road.

## Core Pipeline
1. Collect channel videos
2. Fetch transcripts and metadata
3. Classify usefulness before stock analysis
4. Extract tickers only from analyzable videos
5. Fetch current basic financial snapshots
6. Generate master-style one-line opinions
7. Produce text reports, rankings, and backtests

## Required Gate Contract
Every video must produce:
- `signal_score`
- `video_signal_class`
- `should_analyze_stocks`
- `skip_reason`

Allowed classes:
- `ACTIONABLE`
- `SECTOR_ONLY`
- `LOW_SIGNAL`
- `NOISE`
- `NON_EQUITY`

## Output Contract
For each selected stock, preserve:
- current basic financial state
- key basic indicators summary
- basic verdict
- master-by-master one-line verdicts
- final aggregate verdict
- invalidation triggers

## Master Framework
- Keep the design extensible for multiple masters.
- Initial masters: `druckenmiller`, `buffett`, `soros`

## Session Start Checklist
- Read `AGENTS.md`
- Read `SESSION_HANDOFF.md` if resuming
- Read `~/workspace/WORKSPACE.md`
- Read `README.md` for local commands, config, and scheduler behavior
- Check `git status --short`
- Search memsearch before redoing prior investigation

## Setup And Verification
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .[dev,dashboard]

pytest
pytest --cov=src/omx_brainstorm --cov-report=term-missing
```

## Editing Rules
- Keep diffs small and reversible.
- Prefer existing utilities and patterns over new abstractions.
- Prefer deletion over addition when cleaning up.
- Do not add dependencies unless explicitly requested.
- Do not overwrite unrelated user changes.
- Verify before claiming completion.
