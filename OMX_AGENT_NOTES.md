# OMX Agent Notes

## Current agreed direction
- Operate in `ralph` mode by default.
- Proceed without user intervention; user checks only final outputs.
- Primary workflow: brainstorming -> design -> development -> review -> testing -> final result.

## Core pipeline
1. Collect YouTube channel/videos.
2. Fetch transcripts.
3. Run `video usefulness / signal gate` before ticker extraction.
4. Analyze only videos that pass the gate.
5. Extract tickers.
6. Fetch current basic financial/fundamental snapshot.
7. Generate master-style one-line opinions.
8. Produce text-based final reports.

## Video usefulness gate (must add/use)
Each video should be classified before stock analysis:
- ACTIONABLE
- SECTOR_ONLY
- LOW_SIGNAL
- NOISE
- NON_EQUITY

Required fields:
- `signal_score`
- `video_signal_class`
- `should_analyze_stocks`
- `skip_reason`

## Analysis output direction
For each selected stock, final text output should include:
- current basic financial state
- key basic indicators summary
- basic verdict
- master-by-master one-line verdicts
- final aggregate verdict
- invalidation triggers

## Master framework direction
- Druckenmiller is only the first master, not the only one.
- Architecture should remain extensible for additional masters.
- Current initial masters:
  - druckenmiller
  - buffett
  - soros

## Current execution priority
1. Add/use video usefulness gate.
2. Run recent 5 videos from the selected channel.
3. Skip low-value videos.
4. Analyze only valid videos.
5. Save actual result artifacts.
