# Spruce Leaf: persistent sales operating system

Spruce Leaf should remain the product and decision engine. It should not become
a thin prompt wrapper around a collection of sales SaaS tools, and it should not
fork a full CRM monorepo. The durable advantage is the closed loop it already
owns:

`business strategy → signals → root-cause qualification → people/vantage → outreach → replies → proof → outcome → learning`

The next phase is to make that loop persistent, observable, multi-user, and safe
to run continuously in the cloud.

## What exists now

- Conversational routing across brands and sales motions.
- Business profiles, versioned GTM plays, signal definitions, qualification,
  experiment assignments, proof briefs, and customer-development records.
- Apollo sourcing/enrichment with persisted targeting corrections.
- A local knowledge library with books, video research notes, Harvey material,
  cold-outbound skills, stage-aware retrieval, and required principle lineage.
- Separate planner/writer/reviewer roles plus ten editable critic lenses.
- Review-gated, staged email/LinkedIn sequences that stream into the CRM.
- Suppression, schedules, recipient-local windows, account throttles, mailbox
  caps, replies, meetings, funding outreach, and a leased background-job table.
- Gmail OAuth and mailbox synchronization work in progress.

This is already an unusually broad SDR kernel. Its limitations are mostly
production-system limitations rather than missing prompts.

## The target control plane

```text
CLI / browser / mobile
        │
        ▼
Authenticated API + chat sessions
        │
        ├── Postgres system of record
        │     orgs, users, policies, accounts, people, signals, plays,
        │     sequences, conversations, opportunities, outcomes, audit
        │
        ├── Durable work queue / workflow history
        │     research, enrichment, drafting, approvals, sends, inbox,
        │     meetings, renewal, experiments, reconciliation
        │
        ├── Integration boundary
        │     Gmail, Microsoft Graph, calendars, Apollo, web research,
        │     enrichment waterfalls, CRM import/export, webhooks
        │
        ├── Knowledge and evaluation
        │     versioned sources, retrieval snapshots, datasets, graders,
        │     prompt/model versions, human feedback, outcome attribution
        │
        └── Policy engine
              autonomy level, spend/send budgets, jurisdictions, approvals,
              suppression, quiet hours, kill switches, escalation rules

Workers: orchestrator | researcher | copy/review | delivery | inbox | analytics
```

The text interface remains the primary control surface, but every conversation,
plan, tool action, approval, and result becomes a durable record. A user should
be able to say “find five OutageHub accounts,” close the laptop, and return to a
traceable run that continued within policy.

## Product surface: organize around revenue outcomes

The feature set should not become the navigation. Research, knowledge retrieval,
personas, enrichment, drafting, critics, scheduling, inbox classification, and
experiments are internal capabilities. The user-facing system should organize
them into five outcome loops:

1. **Find the right work:** rank accounts, people, replies, and opportunities by
   expected value, evidence strength, timing, and the next uncertainty to resolve.
2. **Start conversations:** show the smallest set of drafts or manual LinkedIn
   tasks that need approval, with the evidence and goal behind each one.
3. **Create meetings:** turn replies, referrals, objections, and engagement into
   a recommended response, routing decision, and bookable next step.
4. **Advance revenue:** turn meetings into discovery evidence, stakeholder maps,
   proofs-of-concept, proposals, mutual actions, and explicit commitments.
5. **Learn and reallocate:** connect meetings, proof events, opportunities,
   revenue, losses, and corrections to the exact signal, play, sequence, and
   model version that produced them.

The home screen should therefore be a **Today / Next Best Actions** queue, not a
dashboard of everything Spruce Leaf knows. Each item needs: desired outcome,
reason it is prioritized, evidence, confidence, expected effort, risk, owner,
deadline, and one primary action. Pipeline and GTM views remain available for
analysis, but the work queue is where the daily sales process runs.

Every chat command follows one durable interaction contract:

request → interpreted scope → estimated cost/risk → live run → approvals →
external actions → outcomes → learning

The agent should answer a result-oriented prompt such as “get five meetings for
OutageHub this month” with a visible operating plan: the account hypotheses it
will test, current bottleneck, work in progress, approvals needed, meetings
created, and what it changed after real market feedback. Raw tool calls and
synthetic critic scores stay inspectable, but they are not the headline.

## Mac and mobile

Mac is the command center for research comparison, bulk editing, strategy, GTM
experiments, and full pipeline analysis. Mobile is the operating companion for
short, high-value loops:

- talk to the same persistent agent and follow a run after the laptop closes;
- see Today, approvals, replies, meetings, risks, and a compact pipeline;
- approve/edit a draft, complete a LinkedIn task, route a referral, pause sends,
  or record a meeting note;
- open an account, then a contact, then a touch—never pan across the desktop
  spreadsheet;
- receive push notifications only for declared exceptions: positive reply,
  approval deadline, meeting change, provider failure, or policy/budget breach.

The responsive localhost CRM now renders account/contact/touch cards at phone
widths while retaining the dense people sheet on desktop. That is an interim
view, not the cloud mobile product. The real phone surface requires:

- authenticated, organization-scoped API sessions;
- persisted chat messages, plans, run events, and streaming status;
- a bottom navigation model: **Agent, Today, Inbox, Pipeline, More**;
- 44px touch targets, safe-area support, accessible status text, and no
  information that depends on color alone;
- an installable PWA shell initially; native iOS/Android only if notification,
  offline, share-sheet, or device integration requirements justify two more
  clients;
- server-side work continuation, because a mobile browser cannot be the worker.

Do not expose the current loopback dashboard to the public internet to make this
possible. Authentication, RBAC, audit, secret isolation, rate limits, CSRF, and
tenant boundaries come before a public URL. Tailscale is an acceptable private
bridge during the single-user phase, but it is not the multi-user architecture.

## Build versus adopt

| Capability | Decision | Why |
| --- | --- | --- |
| Sales data model and decision engine | Build in Spruce Leaf | Root-cause qualification, vantages, plays, evidence boundaries, and proof loops are the differentiation. |
| Primary CRM UI | Keep building; optionally sync with Twenty | [Twenty](https://github.com/twentyhq/twenty) is a useful reference for configurable objects, views, workflows, permissions, and agents, but embedding its large TypeScript/Postgres monorepo would create two systems of record. Add import/export or a connector later. |
| Durable execution | Strengthen the existing queue first; evaluate Temporal later | [Temporal](https://docs.temporal.io/) is the strongest model for crash-resumable workflows, but its native Rust SDK is still public preview. Postgres leases/outbox/reconciliation give a safer near-term migration. |
| OAuth and integration credentials | Direct Google/Microsoft initially; consider Nango at multi-tenant scale | [Nango](https://github.com/NangoHQ/nango) handles OAuth, refresh, syncs, actions, and many APIs. It becomes valuable once many customers connect many providers. Its self-hosting/license/security model must be reviewed before adoption. |
| Email ingestion | Gmail watch/history + Microsoft Graph change notifications | Event-driven sync is faster and cheaper than polling. Keep a periodic reconciliation poll because providers document delayed/dropped notifications. |
| Email delivery | Gmail API and Microsoft Graph, behind one transport trait | Preserve provider IDs, RFC threading, sent-folder truth, and reconciliation. Keep SMTP only as a legacy adapter. |
| LinkedIn | Manual task surface unless approved partner access exists | LinkedIn invitation/messaging APIs are restricted. Browser automation creates account and compliance risk; Spruce Leaf should prepare and track tasks, not impersonate access it does not have. |
| Scheduling | Existing Google Calendar adapter first; Cal.com connector if booking pages are needed | [Cal.com](https://cal.com/docs/availability) exposes scheduling APIs and self-hosting, but it is not required for the first persistent loop. |
| Knowledge search | Postgres full text + pgvector first; Qdrant only if scale/quality proves necessary | [Qdrant hybrid search](https://qdrant.tech/documentation/search/hybrid-queries/) is strong, but a second database is premature while the corpus is small. Every retrieval must retain source/version/permission lineage. |
| LLM traces, prompt versions, and evals | Adopt Langfuse plus OpenTelemetry | [Langfuse](https://langfuse.com/) supplies traces, datasets, experiments, prompt management, and evaluators. [OpenTelemetry](https://opentelemetry.io/docs/) keeps service telemetry vendor-neutral. |
| Generic no-code automations | Integrate, do not make them the core | Twenty workflows, Activepieces, n8n, or Trigger.dev can handle edge integrations. Revenue-critical state transitions stay typed, tested, and auditable in Spruce Leaf. |

## Autonomy is a policy ladder

Autonomy must be granted per organization, brand, motion, channel, and action—not
as one global “autopilot” boolean.

1. **Observe:** ingest accounts, people, mail, meetings, calls, and outcomes.
2. **Recommend:** rank work and explain evidence, uncertainty, and expected value.
3. **Draft:** create research, sequences, replies, proof briefs, and tasks.
4. **Execute reversible work:** enrich, refresh, schedule, create drafts, and update records.
5. **Execute bounded external work:** send approved classes of messages within budgets and allowlists.
6. **Optimize:** allocate experiments and change plays only inside declared bounds.

Every external action needs an idempotency key, actor, policy decision, model and
prompt version, evidence IDs, cost, timestamps, provider response, and a
reconciliation state. High-risk actions need approval or a narrowly scoped
standing policy. A global pause must stop new sends without preventing inbox
processing, suppression, or reconciliation.

## Cloud migration sequence

### Phase 0 — harden the current single-node system

- Keep one live daemon. Do not horizontally scale SQLite delivery workers.
- Use atomic send claims and leave uncertain/crashed sends in a visible
  reconciliation state rather than retrying blindly.
- Back up the database with SQLite's online backup mechanism, not by copying only
  `sales.db` while WAL writes are active.
- Keep the CRM on loopback/Tailscale. The current UI is not an internet-facing,
  authenticated multi-user application.
- Run a closed-loop allowlisted mailbox test before any production send.

### Phase 1 — separate product API from workers

- Introduce repository traits around the large SQLite module.
- Move schema migrations to checked-in numbered migrations.
- Add `organizations`, `users`, `memberships`, `roles`, `chat_sessions`,
  `chat_messages`, `policy_versions`, `integration_connections`, `secrets_refs`,
  `work_runs`, `work_steps`, `approval_requests`, and `audit_events`.
- Port to Postgres and use transactions, `FOR UPDATE SKIP LOCKED`, advisory locks,
  unique idempotency keys, and a transactional outbox.
- Ship separate `spruce-api` and `spruce-worker` processes from the same Rust
  workspace. The CLI becomes an authenticated API client rather than owning the
  runtime.

### Phase 2 — event-driven communications

- Store OAuth tokens in a cloud secret manager/KMS, never plaintext database
  columns or container files.
- Add Gmail `watch` + Pub/Sub history ingestion and Microsoft Graph webhooks.
- Renew watches/subscriptions automatically and run periodic gap reconciliation.
- Send through provider APIs, persist provider/thread/message IDs, and reconcile
  `sending` rows against the sent mailbox before retrying.
- Add bounce, complaint, unsubscribe, out-of-office, referral, positive-reply,
  and meeting state machines with deduplicated provider events.

### Phase 3 — evidence and evaluation flywheel

- Turn books, skills, calls, sites, emails, and outcomes into versioned sources
  with ownership, permissions, freshness, and citation lineage.
- Distill the raw-only cold-outbound/Harvey imports into reusable principle cards;
  today they are searchable passages but do not have first-class principle IDs.
- Snapshot every retrieval used for a decision so future replays use the same
  semantic inputs.
- Add offline datasets from accepted/rejected drafts and real replies, then gate
  prompt/model releases on deterministic tests, human ratings, and business
  outcomes—not ten synthetic critics alone.
- Attribute outcomes across account, contact, signal, play version, sequence,
  touch, model, and experiment arm.

### Phase 4 — full revenue system

Extend the same account/conversation graph beyond outbound:

- inbound qualification and routing;
- discovery notes, stakeholder maps, mutual action plans, and proof-of-concepts;
- opportunity stages, proposals, pricing/approval, contracts, and forecasting;
- onboarding handoff, expansion signals, renewal risk, and referrals;
- partner, event, funding, and customer-development motions;
- forward-deployed workflow library: problem, required data, build, proof,
  outcome, reusable template, and expansion path.

## Immediate implementation backlog

1. Finish and test direct Gmail send plus sent-folder reconciliation.
2. Add a `doctor` command covering DB integrity, stale `sending` work, OAuth,
   mailbox watches, DNS/authentication, queue leases, backups, model providers,
   and cloud configuration.
3. Persist chat sessions, interpreted scopes, plans, approvals, and run events;
   expose them through an authenticated streaming API so Mac and phone share one
   conversation and one running job.
4. Add the outcome work queue and mobile information architecture: Agent, Today,
   Inbox, Pipeline, More. Measure time-to-qualified-meeting and
   stage-to-stage conversion, not feature usage.
5. Split `db.rs`, `crm.rs`, `outreach.rs`, `engine.rs`, and `sourcing.rs` behind
   narrow interfaces; they currently hold most repository complexity.
6. Add integration tests for two workers, crash-after-provider-send, duplicate
   webhook delivery, token refresh races, suppression arriving during send, and
   policy revocation during a long run.
7. Create the Postgres schema and outbox in parallel with the SQLite adapter;
   switch only after replaying a production-shaped snapshot.
8. Add organization authentication and audit before exposing any dashboard over
   the public internet.

The product goal is not “an AI that sends more email.” It is a governed revenue
runtime that continuously decides what evidence to gather, which account action
is justified, what proof would change the buyer's mind, and what the result
teaches the next run.
