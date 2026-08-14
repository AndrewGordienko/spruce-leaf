# Outreach autoresearch loop

Sales OS treats each account-specific sequence as an experimental candidate.
The loop borrows Pine Leaf's separation between a locked evaluator and an
editable candidate, but applies it to account research and cold-email copy.

## Invariants

- Public evidence, the active GTM play, deterministic copy checks, and the
  skeptical-recipient review rubric stay fixed during a candidate run.
- The editable object is the account-specific sequence. A repair may change
  only the stages named by the evaluator.
- Every audit result is appended to the `events` ledger as
  `copy_research_attempt`, including failed repairs and clean audits.
- Findings are compiled into brand-scoped `copy_research_rule` learnings.
  Rules observed once are provisional; rules observed at least twice are
  durable and are supplied to later candidates.
- A sequence converges only after two consecutive independent audits find no
  actionable problem. The second audit is explicitly asked to falsify the
  first clean result rather than rubber-stamp it.
- A hard model-call budget prevents a subjective loop from running forever.
  `SPRUCE_COPY_RESEARCH_MAX_QA_CALLS` defaults to 8 and is clamped to 4–16.
- Exhausting the budget rejects the candidate. It never weakens the rubric or
  silently approves the last draft.

## Candidate lifecycle

1. Source and qualify an account against the current versioned GTM play.
2. Hold the account if any mandatory evidence foundation is missing.
3. Plan and write a sequence from verified facts and the recipient's vantage.
4. Run deterministic checks for cadence, channels, length, forbidden phrases,
   unsupported claims, repetition, and brand-specific evidence gates.
5. Repair named deterministic defects, then run an independent semantic audit.
6. When the audit finds a material problem, record it, compile the rule,
   regenerate only the affected stages, and repeat the locked evaluation.
7. Save a reviewed draft only after two consecutive clean audits.

For an account already in CRM, rerun the research gate without another Apollo
search:

```bash
spruce-leaf --brand gnk research "Scotlynn" --thesis "refrigerated-load rejection decision"
```

The same action is available to the conversational agent as
`research_account`. It re-reads official evidence and writes a current-play
assessment before any regeneration is attempted.

This is bounded convergence, not a claim that copy can be proven perfect.
Human replies remain the strongest evidence. Reply outcomes are attributed to
the sequence policy and feed future research without rewriting sent history.

## Current brand experiments

GnK requires a specific recurring decision, a believable consequence, an
external trigger or direct mechanism evidence, and a recipient close to the
work. Company category or a generally plausible cross-system workflow is not
problem evidence.

OutageHub covers a governed catalog of distributed operating segments. One
source-backed footprint plus a segment-matched operator may receive a single
premise-testing discovery email. A multi-touch sequence requires an evidenced
outage-time decision and a completed historical match between a verified
operating address and a utility outage area/timestamp. Website research can
now intake, geocode, cache, and match exact first-party addresses for every
physical-location segment; the public EV feed remains the automated catalog
source for charging networks. A four-touch motion additionally requires a
prospect reply whose explicit problem confirmation and exact supporting quote
pass the reply-evidence grade.

Wapahki retains its plant-first task/economics gate. Product variety alone
cannot establish a manual task, automation failure, or economic consequence.
