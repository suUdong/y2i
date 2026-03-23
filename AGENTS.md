# Y2I / OMX Agent Rules

## Default Execution Mode
- Always operate in `ralph` mode by default.
- Proceed without user intervention whenever reasonably possible.
- The user should review final outputs, not intermediate steps.

## Required Delivery Flow
For meaningful OMX work, run the full bounded slice end-to-end:
1. Brainstorming
2. Design
3. Development
4. Review
5. Testing
6. Final result artifact generation

## Execution Discipline
- Do not stop for intermediate approval once implementation starts unless there is a real blocker.
- A real blocker means missing external access, unavailable required data, broken credentials, or a hard technical constraint that cannot be worked around safely.
- If not blocked, continue until a usable result is produced.

## OMX Pipeline Direction
- Collect YouTube channel/videos.
- Fetch transcripts.
- Run a video usefulness / signal gate before ticker extraction.
- Analyze only videos that pass the gate.
- Extract tickers.
- Fetch current basic financial/fundamental snapshot.
- Generate master-style one-line opinions.
- Produce text-based final reports.

## Video Usefulness Gate
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

## Analysis Output
For each selected stock, final text output should include:
- current basic financial state
- key basic indicators summary
- basic verdict
- master-by-master one-line verdicts
- final aggregate verdict
- invalidation triggers

## Master Framework Direction
- Druckenmiller is only the first master, not the only one.
- Architecture should remain extensible for additional masters.
- Current initial masters:
  - druckenmiller
  - buffett
  - soros


## Session Handoff Rule
- If the user indicates work will continue in a new session, save a resumable handoff summary to `SESSION_HANDOFF.md`.
- That handoff must include: current progress, important decisions, blockers/limitations, generated outputs, and the exact next step.
- In a resumed session, read `AGENTS.md` and `SESSION_HANDOFF.md` before continuing.
