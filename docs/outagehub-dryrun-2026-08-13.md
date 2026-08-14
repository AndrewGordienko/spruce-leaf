# OutageHub representative dry-run outputs — 2026-08-13

> **Fixture coverage only; not a pilot release artifact.** These fictional
> comparisons remain useful regression tests, but they do not satisfy the real
> sourcing/generation/approval/SMTP threshold. Run
> `spruce-leaf --brand outagehub pilot-audit` against the execution database.
> That read-only command requires 20 source-backed real accounts across five
> persisted segments, ten current model-generated and explicitly manually
> approved messages, zero current role/copy failures, and at least one SMTP
> delivery to a currently allowlisted controlled inbox.

Eight segment walk-throughs produced without sending email, spending Apollo
credits, or calling paid models. Every readiness state, role verdict, touch
count, lane, and T1 below is **verified by tests** against the production
predicates: the account rows and copy live in
`tests/fixtures/outagehub_eval_2026-08-13.json` and run through
`src/outagehub_eval.rs` (`cargo test outagehub_eval`). Companies are fictional
composites; the evidence strings are shaped like real sourced claims.

Two segments are deliberately **held for research** (multi-site retail, senior
residences) and one is **deprioritized outright** (municipal emergency
management). Not every example passes; that is the point.

Reading key per example: facts → hypothesis → readiness → lane → offer →
recipient → held recipients → allowed touches → T1 → deterministic QA.

---

## 1. Cold storage — Northline Cold (fixture: `ops_owner_with_exact_historical_match`)

- **Sourced facts:** operates six automated cold-storage facilities across
  Ontario/Alberta; maintenance checks utility outage status before deciding
  generator runtime; completed historical match — verified warehouse at
  41 Rail Side Rd, Brampton fell inside Alectra's reported outage area
  beginning 2026-01-18T02:14Z.
- **Hypothesis (unverified):** the utility check is a manual lookup rather
  than part of the alarm chain.
- **Readiness:** `action_ready` (decision + owner + completed match).
- **Lane:** **easy** — 220 headcount, replay runs without integration.
- **Offer:** Historical Location Replay (CAD $2,000–$15,000).
- **Recipient:** Maintenance Manager (`process_owner`, direct role fit).
- **Held:** finance, QA (later-stage; problem not yet confirmed).
- **Allowed touches:** 2 (T2 carries the dated historical result).
- **T1 (74 words, QA: pass):** fixture `valid_discovery_t1_cold_storage` —
  subject “Power checks at Brampton cold storage”.
- **Deterministic QA:** pass. The 150-word variant and the
  “Utility outage visibility” subject variant of this same account **fail**
  (fixtures `one_hundred_fifty_word_t1`, `generic_outage_visibility_subject`).

## 2. Generator services — PrairiePower Rentals (`generator_company_with_dispatch_evidence`)

- **Facts:** dispatches technicians and a rental generator fleet during
  emergency response; dispatch supervisor checks utility outage status and
  restoration estimates before staging crews; completed match at the Red Deer
  site (FortisAlberta, 2026-02-03T21:40Z).
- **Hypothesis:** call volume arrives before any outage picture does.
- **Readiness:** `action_ready`. **Lane: easy** (85 headcount).
- **Offer:** Historical Location Replay of last storm season vs. service territory.
- **Recipient:** Dispatch Supervisor (`operator`, direct).
- **Held:** owner/GM (economic, later), equipment sales (unsuitable).
- **Allowed touches:** 2.
- **T1/T2 (70 + 60 words, QA: pass):** fixture `dryrun_generator_two_touch` —
  T2 contributes the dated FortisAlberta result with the outside-context boundary.

## 3. EV charging — ChargeField (`ev_charging_ops_owner_discovery`)

- **Facts:** operates/monitors a Canadian charging network; support decides
  whether a charger incident needs field-service escalation after checking
  utility outage status. No historical match yet.
- **Readiness:** `discovery_ready`. **Lane: medium.**
- **Offer:** API Evaluation after a public-station replay.
- **Recipient:** Charging Operations Manager (`process_owner`, direct).
- **Held:** Platform Engineering Lead (technical, requires confirmed problem —
  fixture `technical_evaluator_before_confirmation`); Director of Sales
  (route-only — fixture `sales_contact_route_only`).
- **Allowed touches:** 1.
- **T1 (71 words, QA: pass):** fixture `dryrun_ev_charging_t1` — subject
  “Charger alarms during grid events”.

## 4. Telecom — Trilliant Telecom (`telecom_noc_owner_discovery`)

- **Facts:** tower network with NOC monitoring across northern Canada; the NOC
  decides whether a tower alarm is grid power loss before dispatching a crew.
- **Readiness:** `discovery_ready`. **Lane: medium.**
- **Offer:** API Evaluation; storm-season replay of tower coordinates first.
- **Recipient:** Director Network Operations (`process_owner`, direct).
- **Held:** Director of Human Resources (blocked despite a model-supplied
  `process_owner` vantage — fixture `hr_contact_mislabelled_operational`);
  VP Finance (requires confirmed problem — `finance_before_problem_confirmation`);
  Software Developer II (broad Apollo fallback, not near the decision —
  `broad_apollo_fallback_employee`).
- **Allowed touches:** 1.
- **T1 (72 words, QA: pass):** fixture `dryrun_telecom_t1` — subject
  “Tower dispatch after power loss”.

## 5. Laboratories / healthcare — Beacon Labs (`ops_owner_without_match`)

- **Facts:** operates laboratories and service centres processing clinical
  specimens across Ontario; site operations checks utility outage status
  during a lab power interruption before couriering specimens.
- **Readiness:** `discovery_ready` — no historical match yet, so the old
  system's seven-touch campaign is now exactly one email.
- **Lane: medium.** Offer: address replay, then API Evaluation.
- **Recipient:** Director of Operations (`process_owner`, direct).
- **Allowed touches:** 1.
- **T1 (69 words, QA: pass):** fixture `dryrun_labs_t1`.

## 6. Insurance CAT claims — LakeShore Mutual (`insurance_cat_claims_owner_discovery`)

- **Facts:** regional claims operations handling storm response; CAT claims
  operations checks utility outage reports to verify power-loss windows on
  policyholder storm claims.
- **Readiness:** `discovery_ready`. **Lane: medium.**
- **Offer:** historical replay of one named CAT event's claimed locations.
- **Recipient:** Director, CAT Claims Operations (`process_owner`, direct).
- **Held:** Property Underwriting Director — generic CAT exposure is never a
  utility-data workflow (fixture `insurer_generic_cat_exposure` stays
  `research_required`).
- **Allowed touches:** 1.
- **T1 (73 words, QA: pass):** fixture `dryrun_insurance_t1`.

## 7. Senior residences — Harbourview Residences (**HELD**, `residence_operator_broad_continuity_claims`)

- **Facts:** operates fourteen retirement residences in BC; publishes a
  commitment to resident safety and emergency preparedness.
- **Why held:** a public emergency-plan commitment is exposure, not a decision.
  No source names who verifies a utility event or what changes when they do.
- **Readiness:** `research_required`. **Lane: research.** **Touches: 0.**
- **Next missing fact:** a source naming the facility/emergency-preparedness
  function's actual power-event response (not a values page).

## 8. Multi-site retail — Maple Retail Group (**HELD**, `distributed_exposure_without_decision`)

- **Facts:** operates 240 grocery stores across seven provinces; website lists
  locations and hours.
- **Why held:** distribution alone was the old system's favourite trap — it
  authorized “which sites get checked first?” theory at unrelated companies.
  There is no evidenced outage-time decision here.
- **Readiness:** `research_required`. **Lane: research.** **Touches: 0.**
- **Next missing fact:** evidence that central/store operations (not
  merchandising) actually scopes outages centrally today.

## Bonus hold — Municipal emergency management (`municipal_eoc_deprioritized`)

Real decision evidence exists (EOC monitors outage reports for alerting), but
the segment is **deprioritized**: no public-sector-ready offer, slow political
procurement. Role gate returns false for everyone; state stays
`research_required`; the lane is `hard` with the deprioritization named in
`next_missing_fact`. Research may continue; outreach may not.

---

## The skeptical-recipient test applied to the passing T1s

Each passing T1 was checked against the nine questions in the brief:

1. *Do I recognize the event?* Yes — charger offline, tower unreachable,
   warehouse power loss, storm claim file: each is the segment's own event,
   not a universal “site goes dark” fork.
2. *Is it my responsibility?* Recipients are the deterministic role-gate
   survivors; HR/finance/sales/underwriting variants are held by tests.
3. *Is the claim supported?* Every company-specific statement traces to the
   fixture's evidence strings; the mechanism is framed as the thing being asked.
4. *Is the uncertainty honest?* Each T1 names its own falsification
   (“if telemetry already answers this, that is a useful answer too”).
5. *Does OutageHub do something concrete?* One plain clause: matched Canadian
   utility outage reports by location and time through an API.
6. *Can I answer in one sentence?* Every T1 asks exactly one either/or
   operating question.
7. *Does replying give me value?* The reply either kills a vendor thread
   cheaply or surfaces a lookup they are paying for in time.
8. *Written for my company?* Segment event + named footprint fact per account.
9. *Would Andrew say it naturally?* 60–95 words, no strategy language — and
   the deterministic linter, not this document, is the enforcement.
