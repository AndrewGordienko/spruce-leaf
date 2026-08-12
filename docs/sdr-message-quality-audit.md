# SDR message-quality audit

Audit date: 2026-08-10
Scope: live SQLite-backed cold-outreach path, ten persisted sequences, scheduling, and outcome learning
Data handling: read-only inspection of `.spruce/sales.db`; personal and company names are redacted below

## Executive summary

The first material quality break was upstream of the copywriter. Accounts with no current play assessment or only a `research_needed` assessment could retain active sequences; the manual approval path then treated copy approval as commercial authorization. The database snapshot contained 12 scheduled touches, including sequences with no current assessment. It also contained active sequences for accounts far beyond the configured employee ceiling. Before this remediation, the cadence trusted those scheduled rows instead of re-evaluating them immediately before SMTP.

The second break was recipient/required-response mismatch. Router, economic-buyer, and technical-evaluator contacts could enter the same four- or seven-touch sequence path as workflow owners. A prose instruction asked routers to route, but no deterministic control enforced it. Model `primary` judgments and email verification could also outrank stronger title-supported workflow relevance.

The writer and reviewer are not the primary root cause. The system already had good evidence boundaries, atomic draft promotion, current-copy-policy checks, deterministic lint, independent verification, delivery windows, capacity controls, and an atomic send claim. Two qualified, correctly routed traces scored 50–51/60. The weak traces failed mainly on account fitness, problem support, or recipient fit before prose quality became decisive.

The high-confidence gaps are now implemented:

- manual approval and final delivery share one current GTM gate;
- stale or unsafe scheduled rows are blocked before SMTP;
- refresh can reject weak inventory using the same evidence policy as initial qualification;
- routers are held out of automated cadences, and later-stage buyers/evaluators wait for a prospect-confirmed problem;
- role relevance dominates model-primary and email-availability tie-breakers;
- website evidence extraction uses the substantive model lane;
- replies and nonresponse snapshot the originating touch, role, hypothesis, play, experiment, copy policy, backend, and model.

The production snapshot has zero sent touches, replies, GTM outcomes, experiments, or assignments. Therefore this audit makes no causal claim about reply performance, reviewer scores, or a winning copy pattern. The 246 rows in `learnings` are pre-send sourcing/qualification learnings, not market outcomes.

## Actual runtime path

The send-capable path is:

1. Real Apollo account sourcing enters at `src/sourcing.rs:247`.
2. Website evidence is extracted at `src/research.rs:235`.
3. Account qualification, hypothesis, and structured signals are produced by `qualify_org` at `src/sourcing.rs:1469`, then constrained by deterministic qualification rules.
4. Contact role/vantage mapping is produced by `assign_vantage` at `src/sourcing.rs:1552`, with shared deterministic classification in `src/response_design.rs:80`.
5. Email enrichment enters at `src/enrich.rs:56`.
6. `gtm::prepare_action` resolves the current play, current assessment, live evidence, selected contact, and stable experiment arm at `src/gtm.rs:485`.
7. Live planning enters at `src/outreach.rs:716`; contact-stage eligibility is checked at `src/outreach.rs:770`.
8. One recipient-specific sequence is written at `src/outreach.rs:1407`.
9. Deterministic touch and sequence checks run at `src/outreach.rs:3342`, `src/outreach.rs:3797`, and `src/outreach.rs:3814`.
10. The active edit/verify loop is `review_and_edit_sequence_lean` at `src/outreach.rs:1908`. The ten-lens council is optional and off by default at `src/outreach.rs:1891`; the legacy reviewer at `src/outreach.rs:2372` has no live caller.
11. Draft finalization and optional automatic scheduling run at `src/outreach.rs:1203`. Manual approval now enters the same delivery policy at `src/outreach.rs:68`.
12. Portfolio scheduling and capacity placement run at `src/calendar.rs:72` and `src/calendar.rs:316`.
13. The cadence rechecks GTM eligibility at `src/cadence.rs:108`, recipient-local timing at `src/cadence.rs:223`, account fan-out at `src/cadence.rs:262`, and wins an atomic send claim at `src/cadence.rs:334`.
14. Inbox polling enters at `src/inbox.rs:26`; thread-aware handling is at `src/reply_agent.rs:87`; attributed reply learning is at `src/reply_agent.rs:496`.

`pipeline::simulate` is a separate, explicitly synthetic prompt-experiment path. It does not populate either CRM or the SQLite send queue and must not be used to explain production delivery behavior.

## Ten-message trace

Scoring is an audit judgment, not measured market performance. Each dimension is 0–5. Vector order: account fit (AF), problem specificity (PS), evidence quality (EQ), recipient fit (RF), relevance/opening (RO), seller differentiation (SD), reason to reply (RR), CTA quality (CTA), naturalness (N), sequence progression (SP), evidence safety (ES), overall commercial quality (O).

| ID | Persisted trace, redacted | Short stage-one excerpt | Score vector | Total | Earliest break |
|---|---|---|---:|---:|---|
| T1 | 840-person logistics provider; no current assessment; operations executive; mixed draft/scheduled | “When a client asks for an explanation of a shipment issue…” | 3/2/2/4/3/2/3/3/4/3/4/1 | 34/60 | Current-assessment and approval boundary |
| T2 | 190-person logistics provider; fit 43; `research_needed`; operations executive; mixed draft/scheduled | “When a completed shipment needs an explanation…” | 2/2/2/4/3/2/3/3/4/3/4/2 | 34/60 | Problem/evidence gate |
| T3 | 57-person solar provider; fit 25; `research_needed`; commercial router; seven touches | “When a commercial solar site … drops to zero … the dispatch-versus-hold call…” | 3/3/2/0/3/3/1/0/3/2/2/0 | 22/60 | Recipient and required response |
| T4 | 142k-person enterprise; fit 28; `research_needed`; finance buyer; seven touches | “I wondered whether finance/risk ever needs a clear record trail…” | 0/1/1/0/2/2/2/1/3/2/3/0 | 17/60 | Account ceiling, then recipient stage |
| T5 | 57-person solar provider; fit 25; `research_needed`; technical evaluator; seven touches | “Monitoring and comms fields … shape what is available in those first minutes.” | 3/3/2/1/4/3/2/2/4/3/3/1 | 31/60 | Technical evaluation before problem confirmation |
| T6 | 350-person cold-storage operator; fit 73; qualified; HR/health-and-safety contact; 21 touches across drafts | “Your national H&S escalation path starts under product-risk pressure.” | 4/3/4/0/3/3/2/1/4/3/4/1 | 32/60 | Contact mapping/role evidence |
| T7 | Same cold-storage operator; qualified maintenance owner; mixed draft/scheduled | “Maintenance has to decide whether to start with the facility itself or consider a wider utility event…” | 5/4/4/5/4/4/4/4/4/4/5/4 | 51/60 | No material upstream break found |
| T8 | 170-person food producer; fit 79; qualified CI owner; mixed draft/scheduled | “Is there a repeatable packing or handling step where people place finished product…” | 5/4/4/5/4/3/4/4/4/4/5/4 | 50/60 | No material upstream break found |
| T9 | 84-person claims administrator; fit 24; `research_needed`; CEO; seven touches | “When a closed claim is reviewed, does the file itself … show what evidence supported the decision?” | 1/1/1/0/2/2/2/1/3/2/4/0 | 19/60 | Account/problem support, then recipient stage |
| T10 | 35k-person enterprise; no assessment; supply-chain executive; seven touches | “When a retailer issues a shortage … deciding whether to dispute or write it off…” | 0/2/2/3/3/3/3/2/3/3/2/0 | 26/60 | Account ceiling and current assessment |

These traces show that fluent, evidence-conscious copy can still be commercially unsafe when the preceding account or recipient decision is wrong. T7 and T8 also show the opposite: when the account, problem, and person align, the same writing system produces a much stronger result.

## Ranked root causes

### 1. Copy approval bypassed GTM authorization — P0, fixed

The former low-level bulk approval updated every review-passing draft without consulting the current play assessment, evidence state, employee ceiling, or recipient stage. All three approval surfaces called it. The cadence then trusted `scheduled` as sufficient authorization.

The replacement enumerates reviewed recipients and calls the shared `delivery_block_reason` before scheduling (`src/outreach.rs:68`, `src/gtm.rs:453`). CLI, agent, and CRM approval now use it at `src/main.rs:773`, `src/agent.rs:2089`, and `src/crm.rs:1231`. The cadence repeats the same check immediately before delivery at `src/cadence.rs:108`. The low-level SQL transition is crate-private at `src/db.rs:1999`.

### 2. Refresh had a weaker definition of qualification — P1, fixed

Initial sourcing could filter unsupported/low-confidence signals and reject an account. Refresh previously had only `qualified` and `research_needed`, so weak inventory could survive indefinitely and be reused.

`enforce_refresh_qualification` now applies the active catalog, non-empty evidence, confidence ≥0.60, brand credibility rules, hard disqualifiers, fit floors, and a real `rejected` state (`src/sourcing.rs:1023`). Reuse excludes rejected leads and current-play rejected assessments (`src/sourcing.rs:2070`).

### 3. Recipient stage was prompt-only — P1, fixed

Routers, finance/economic buyers, enterprise executives, and technical evaluators could receive the same cadence as process owners. The action gate also counted later-stage vantages as reachable workflow ownership.

The shared classification now identifies route-only and post-confirmation roles (`src/response_design.rs:102`). Planning holds routers for one manual routing request and holds later-stage roles until a prospect-confirmed problem exists (`src/gtm.rs:401`, `src/gtm.rs:426`, `src/outreach.rs:768`). Only operator, frontline, process-owner, and operational-executive roles can satisfy workflow discovery at `src/gtm.rs:537`.

### 4. Ranking weights overrode role evidence — P1, fixed

The reuse path once added 80 points for a verified email, more than the whole role-relevance spread. A model-set `primary` flag added 100 points, contradicting the comment that it was only a tie-breaker.

Role priority is now multiplied before a 1–2 point email tie-breaker (`src/sourcing.rs:2196`). Model-primary adds only two points within a non-routing role (`src/response_design.rs:80`). Sales/revenue/business-operations titles are resolved before generic commercial terms, and generic `data` no longer forces a technical classification (`src/response_design.rs:226`).

### 5. Evidence extraction used the weakest model lane — P1, fixed

Website research is factual substrate for qualification and hypothesis formation. It previously used the fast/economy lane. It now uses the substantive structured lane at `src/research.rs:235`. Qualification and routing remain independently constrained by deterministic policy.

### 6. Outcome records could not answer “what failed?” — P1, fixed prospectively

The old outcome schema linked a sequence/play/experiment but did not snapshot the originating touch, role/vantage, account hypothesis, play version, experiment arm, copy-policy version, generation backend, or generation model. It also did not record terminal nonresponse.

The persisted attribution contract is now explicit in `Sequence` and `GtmOutcome` (`src/db.rs:125`, `src/db.rs:351`), migrated at `src/db.rs:752`, and written at `src/db.rs:4075`. Replies resolve the parent message to a touch and snapshot all fields (`src/reply_agent.rs:507`). Cadence records a deduplicated nonresponse outcome when the unanswered cap stops a sequence (`src/cadence.rs:178`, `src/cadence.rs:412`). This is prospective: there is no historical reply data to backfill.

### 7. Sparse/no market outcomes make automatic optimization unjustified — P2, open by design

The snapshot contains 0 replies, 0 GTM outcomes, 0 running experiments, and 0 assignments. Synthetic reviewer judgments can enforce policy and catch defects, but cannot demonstrate account, role, or copy performance. The writer already reads attributed human outcomes at `src/outreach.rs:3168`, labels patterns under 20 observations provisional, and refuses to invent a winner when none exist.

Do not automatically reweight ICPs or roles from nonresponse alone. Once comparable outcome volume exists, run one-variable experiments through the enforced experiment contract at `src/db.rs:3774`, stable assignment at `src/db.rs:3998`, and sample/time-gated evaluation at `src/db.rs:3908`.

### 8. Multiple contacts can still create unnecessary account pressure — P2, mitigated

Primary/secondary status is now only a ranking tie-breaker, not an activation policy. Multiple correctly classified operators may still have drafts. The cadence mitigates this with a new-front account throttle (`src/cadence.rs:262`) and portfolio capacity scheduling. A future change could activate one workflow owner first and release a second only after a routing signal or nonresponse, but this should be tested rather than assumed universally superior.

## Persona and prompt conflicts

| Conflict | Runtime consequence | Resolution |
|---|---|---|
| Persona doctrine says routers should only route; writer prompt repeated the rule; no code enforced it | Some routers received full seven-touch operational pitches | Deterministic planning hold now wins over prose |
| Economic/technical personas belong after problem confirmation; action readiness counted them as workflow owners | Finance and technical contacts could unlock or enter cold discovery | Reachable-owner and recipient-stage contracts now exclude them until confirmation |
| `primary` was described as a tie-breaker but outweighed role class | Model judgment could promote the wrong person | Two-point tie-break only |
| Discovery-ready copy was safe for review, but approval treated review as authorization | A research hypothesis could become scheduled email | Current `action_ready` is required at approval and delivery |
| The optional ten-critic council sounds authoritative but has no outcome calibration | More synthetic agreement could be mistaken for commercial evidence | It remains opt-in; deterministic lint plus independent final verification remain active |
| The active lean backend folds planning into writing and uses an economy edit pass | Less independent strategy diversity | Retained as an explicit cost/latency tradeoff; final verification and deterministic gates remain independent |

The psychology/persona material is useful when treated as a response-cost contract, not as permission to infer personality. The deterministic classifier and stage gates now control routing; prompts refine message shape inside that boundary.

## Contact routing findings

- Learning-placement titles are still deterministically downgraded to route-only and non-primary.
- Title evidence overrides contradictory model labels for explicit executive, finance, operations, and learning-role signals.
- Role relevance is canonical across sourcing reuse and planning.
- Routers no longer enter automated cadences; a human can use the held reason to send one bounded routing request.
- Economic leaders, enterprise executives, and technical evaluators require a prospect-confirmed problem, derived from customer-development stage or a verified `conversation.problem_confirmed` signal.
- Unknown or novel titles still fall back conservatively to router. This can create false negatives, but not an owner-level false positive.

## Code defects and failure modes

| Defect | Failure mode | Status |
|---|---|---|
| Ungated bulk approval | Review-passing but unqualified/oversized contacts become scheduled | Fixed |
| No send-time revalidation | Stale scheduled rows proceed after play/evidence changes | Fixed |
| Refresh cannot reject | Weak inventory persists forever | Fixed |
| Missing current assessment accepted by action gate | Legacy accounts bypass the current play | Fixed |
| Later-stage roles count as workflow owners | Wrong person unlocks a motion | Fixed |
| Email/model-primary dominate ranking | Reachable but irrelevant contact displaces owner | Fixed |
| Website extraction on fast lane | Weak factual substrate contaminates downstream reasoning | Fixed |
| Outcome lacks touch/role/hypothesis/model | Cannot distinguish targeting, routing, and copy failures | Fixed prospectively |
| No nonresponse outcome | Silent sequences disappear from learning | Fixed prospectively |
| Readiness helper expired observations during a nominal read | Dry-run/read paths could mutate evidence state | Fixed; active-list expiry filtering remains authoritative |

## Learning and evaluation gaps

The closed loop is currently structurally ready but empirically empty:

- real reply learning records positive replies, corrections, referrals, objections, meetings, problem confirmation, and proof-brief creation;
- terminal nonresponse is now recorded once per sequence;
- each outcome can be segmented by account hypothesis, contact title/vantage, touch stage, play/version, experiment arm, copy policy, and generation model;
- current-policy human outcomes are available to future writers as aggregate shape evidence;
- experiment creation rejects missing variables/descriptions/constants, only one experiment can run per play, stable hashing prevents arm drift, and combined/missing-sample tests cannot be declared winners;
- the offline gold/eval suite is still mainly synthetic commercial judgment plus deterministic lint. It is useful for regressions, not proof of market quality.

The next meaningful evaluation is operational: send only action-ready sequences, collect enough comparable outcomes, and compare positive-reply and correction/referral rates by one isolated variable. Until then, reviewer score should remain a sendability safeguard, not a KPI.

## Working safeguards

- real organizations originate in Apollo, not the demo campaign generator;
- account employee ranges are clamped at sourcing and checked again before delivery;
- canonical evidence requires an active signal definition, evidence text, confidence ≥0.60, and brand-specific credibility;
- current play assessment and current evidence are recomputed before copy, approval, and delivery;
- buyer-facing copy cannot promote a private hypothesis to fact;
- exact touch count, required stages, signature, subject, length, repetition, question count, fabricated collateral, and evidence safety are deterministic checks;
- a building sequence is checkpointed and atomically promoted, so a failed rewrite cannot destroy the prior active draft;
- stale copy-policy versions cannot be approved or atomically claimed for sending;
- recipient-local windows, daily business/mailbox capacity, and account-front throttles are enforced;
- a due-touch snapshot still needs a single-winner atomic claim before SMTP;
- replies, opt-outs, and accepted calendar slots have deterministic boundaries outside the model;
- experiments have durable person-level assignments and minimum sample/time gates.

## Recommended fixes and implementation record

| Priority | Category | Confidence | Evidence | Blast radius | Files | Smallest defensible change | Verification | Expected improvement | Status |
|---|---|---:|---|---|---|---|---|---|---|
| P0 | Safety | High | Scheduled/no-assessment and oversized active traces; former approval SQL | All cold delivery | `gtm.rs`, `outreach.rs`, `cadence.rs`, approval callers | One shared current-delivery gate at approval and SMTP | Oversized-account approval test; full suite | Prevent policy-bypassing sends | Implemented |
| P1 | Qualification | High | Refresh-only weak assessments and no prior reject branch | Reused accounts | `sourcing.rs` | Apply initial evidence policy to refresh and persist rejection | Reject/weak-signal unit test | Removes stale weak inventory | Implemented |
| P1 | Routing | High | Router/economic/technical seven-touch traces | Recipient selection | `response_design.rs`, `gtm.rs`, `outreach.rs` | Code-level route-only and problem-confirmation gates | Role-stage tests | Lower wrong-person outreach and reply cost | Implemented |
| P1 | Ranking | High | Former +80 email and +100 primary weights | Reuse/planning pool | `response_design.rs`, `sourcing.rs` | Make availability/primary true tie-breakers | Ranking tests | Keeps workflow owners in scope | Implemented |
| P1 | Evidence | Medium-high | Weak model produced qualification substrate | All newly researched accounts | `research.rs` | Use substantive structured lane | Compile/full suite | More reliable evidence extraction | Implemented |
| P1 | Learning | High | Outcome schema lacked touch/role/hypothesis/model; no nonresponse | Measurement and experiments | `db.rs`, `outreach.rs`, `reply_agent.rs`, `cadence.rs` | Snapshot generation and decision context; add terminal nonresponse | SQL round-trip plus full suite | Separates account, person, touch, and model effects | Implemented prospectively |
| P2 | Portfolio sequencing | Medium | Several secondary contacts had full drafts; send throttle already limits fronts | Account-level cadence | `outreach.rs`, `cadence.rs` | Test one-owner-first release policy against current throttle | Outcome experiment after sufficient sends | Potentially less account pressure | Deferred pending data |
| P2 | Model/eval policy | Medium | Planner/council configuration is cost-sensitive; no real outcomes | Writer/reviewer | `engine.rs`, `outreach.rs`, eval harness | Compare one model-lane change at a time | Blind eval plus live single-variable experiment | Evidence-based quality/cost choice | Deferred pending data |

### P0

- No unresolved P0 remains in the reviewed live cold-email path.
- Existing unsafe scheduled rows were deliberately not rewritten during this audit. The new cadence gate will mark them blocked when processed; operators can also re-plan after refreshing the account.

### P1

- The confirmed qualification, routing, ranking, evidence-lane, approval, delivery, and attribution gaps are implemented.
- Historical outcomes cannot be backfilled because the snapshot contains no replies or sends.

### P2

- One-owner-first portfolio sequencing and model-lane/council changes need outcome data. They are hypotheses, not safe universal fixes.
- Expand title fixtures as real false positives/negatives appear; conservative router fallback is safer than guessing authority.

## Verification

- `cargo check --all --locked`: passed.
- `cargo test --all --locked`: 205 passed, 0 failed at the implementation checkpoint.
- New regression coverage includes unsafe manual approval, refresh rejection, role-first ranking, model-primary tie-breaking, sales-operations and people-function classification, later-stage role gating, and outcome-attribution round trips.
- No binary, SMTP sender, Apollo request, paid model call, or mutation of `.spruce/sales.db` was used for this audit/verification.
