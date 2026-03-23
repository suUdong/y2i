# 01 Office Hours

## Project read
- `y2i/omx-brainstorm` is not fundamentally a YouTube summarizer.
- It is an attempt to convert noisy Korean finance and macro video content into structured, testable investment signals.
- The current workflow is: collect channel videos, fetch transcript or metadata, classify usefulness, extract stocks directly or indirectly, enrich with fundamentals, attach master-style opinions, rank across videos, and compare outcomes.
- The project is therefore trying to bridge three broken workflows:
- information intake
- investment signal extraction
- ex post validation

## What problem is it really solving?
- The obvious framing is “analyze finance YouTube videos.”
- That is too shallow and too tool-centric.
- The deeper problem is: serious retail or semi-pro investors consume a large amount of narrative content, but they do not have a reliable way to transform that content into a structured decision log.
- They hear many strong claims.
- They hear many stock names.
- They hear many macro narratives.
- But they cannot easily answer:
- what exactly was said
- what signal was actionable
- what stock or sector it implied
- whether that signal actually worked

## Why this matters
- Finance content is abundant, persuasive, and highly narrative.
- The bottleneck is not access to opinions.
- The bottleneck is disciplined conversion from narrative to traceable investment hypotheses.
- Most viewers either:
- passively consume content and forget the details
- cherry-pick stock names emotionally
- or manually track thesis notes in an inconsistent spreadsheet or notebook

## Is there real demand?
- Yes, but not for the current surface framing.
- There is demand for “content-to-decision infrastructure” among:
- active individual investors
- small subscription research communities
- family office analysts
- discretionary traders who track sentiment-heavy channels
- creators who want to audit their own historical calls

## Where demand is weak
- Demand is weak for a generic “AI summarizes YouTube” product.
- That category is crowded, low-value, and easy to replace.
- Demand is also weak for a fully automated stock-picker promise.
- Users do not trust a black-box stock recommender unless it has a very clear audit trail.

## Better framing
- Better framing is:
- “Turn finance content into structured, backtestable signal logs.”
- Or:
- “A research operating system for analyst/creator content.”
- Or:
- “Narrative-to-portfolio intelligence for retail and independent investors.”

## Stronger product language
- Current product language over-indexes on transcript and extraction mechanics.
- Stronger language would emphasize:
- evidence
- reproducibility
- thesis invalidation
- channel-level signal quality
- creator-level hit rate

## The real user
- The real user is not “anyone who watches finance YouTube.”
- The real user is someone who already has a workflow and pain.
- Best initial user profiles:
- a high-agency retail investor who tracks 5 to 20 channels
- a research assistant for a Korean investment newsletter
- a creator or analyst who wants to know which content streams produce real alpha

## What the user actually wants
- They do not want a transcript.
- They do not want another summary.
- They want:
- a clean list of claims
- linked sectors and stocks
- entry timing context
- current fundamentals
- explicit invalidation conditions
- and evidence on whether that content stream is worth following

## Narrowest wedge
- The narrowest credible wedge is:
- “Score and audit a small set of Korean semiconductor / macro-finance YouTube channels.”
- Why this wedge works:
- the user set is concrete
- the content format is recurring
- the sectors are narrow enough to map manually
- the backtest and creator comparison angle is compelling

## What is the anti-wedge?
- “Support every finance creator and every market regime.”
- That is too broad.
- The product loses trust if the extraction logic is shallow across too many domains.

## What’s special if this works?
- If it works, this becomes less like a summarizer and more like:
- a creator-signal audit layer
- a disciplined thesis journal
- and eventually a portfolio research cockpit

## Why the current implementation is interesting
- The project already contains the seeds of a stronger product:
- signal gating
- direct and indirect extraction
- fundamentals enrichment
- master opinions
- ranking
- backtest
- cross-channel comparison

## The missing product move
- The missing move is not “more extraction.”
- It is “more trust.”
- Trust comes from:
- narrower domain claims
- clearer evidence chains
- explicit quality scores
- and a product that says “this creator is worth 20 minutes a day” instead of “here are 8 tickers.”

## What I would tell the team in YC office hours
- You are not building an AI YouTube tool.
- You are building a signal audit system for narrative-driven investing.
- The value is not more content processing.
- The value is helping users decide which content sources deserve capital allocation attention.

## Product redefinition
- Redefine the project as:
- “An operating system that converts finance creator content into structured, evidence-backed, backtestable investment signals.”

## Demand test
- The fastest demand test is not general distribution.
- It is:
- pick 3 to 5 channels
- focus on one sector cluster
- show creator-by-creator signal quality
- and ask whether users would pay to replace manual note-taking and intuition with this system

## What to avoid
- Avoid promising predictive alpha too early.
- Avoid pretending the system is already general.
- Avoid a feature list that sounds like an LLM demo.

## Best next product question
- “Can we become the default tool for deciding which Korean finance creators and content streams are worth trusting for sector-specific investment work?”

## Summary
- There is real demand.
- The current product framing is too implementation-centric.
- The better framing is a signal audit / research operating system.
- The narrowest wedge is a small set of repeatable channels in a narrow sector domain.
- The strongest user value is creator selection and thesis tracking, not transcript summarization.
