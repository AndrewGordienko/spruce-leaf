# spruce-leaf

A **"Codex for sales."** Launch it, type what you're hunting for, and it runs the whole
play: find accounts with an expensive workflow, map the people positioned to see that workflow,
and activate one primary contact with a hypothesis-led outreach sequence — grounded in a library of real sales books —
then files everything in a local CRM.

It's a Rust CLI with a provider-neutral reasoning engine. Each line you type goes directly to the
OpenAI Responses API by default, or to the optional Claude, Codex, or Grok local CLI, and can operate:
Apollo sourcing, contact enrichment, verified outreach planning, approval, and funnel reporting.

Set `OPENAI_API_KEY` for the default backend. The normal model is `gpt-5.6-terra`; lightweight
routing uses `gpt-5.6-luna`. The isolated outreach writer uses `gpt-5.6-sol` at `xhigh`; copy repair
uses the selected model at `medium`, and independent final verification uses it at `high`.
Account qualification, contact-vantage selection, hypothesis refresh, and outreach angle planning
use the selected model at `medium`. Sol is intentionally limited to the one call where prose taste
matters; the deterministic sequence gates still decide whether its output is sendable. Override the lanes with
`SPRUCE_OPENAI_WRITER_MODEL`, `SPRUCE_OPENAI_EDITOR_MODEL`, `SPRUCE_OPENAI_VERIFIER_MODEL`
and their matching `*_REASONING_EFFORT` keys. `SPRUCE_OPENAI_COPY_MODEL` and
`SPRUCE_OPENAI_COPY_REASONING_EFFORT` remain fallbacks for all three lanes. Use
`SPRUCE_OPENAI_STRATEGY_MODEL`, and `SPRUCE_OPENAI_STRATEGY_REASONING_EFFORT`. Selecting Sol explicitly
with `--model gpt-5.6-sol` still applies it to other substantive work. Authenticated `claude`, `codex`, and `grok` CLIs remain available through
`--backend`. Apollo and mailbox credentials are still required for real sourcing and delivery.
Frontier outreach calls are globally limited to two concurrent requests; tune this cautiously with
`SPRUCE_OPENAI_FRONTIER_CONCURRENCY`. A turn also stops at 100 model attempts, 120,000 output tokens,
or $2.00 of measured model cost by default. Override those safety rails with
`SPRUCE_TURN_MAX_MODEL_ATTEMPTS`, `SPRUCE_TURN_MAX_OUTPUT_TOKENS`, and
`SPRUCE_TURN_MAX_COST_USD`. Router fallback is one-call-only: it never silently changes the provider
used by the bulk action it routes.
Automatic account-framing refreshes reuse source-backed research for six hours by default; tune with
`SPRUCE_ACCOUNT_REFRESH_TTL_SECS` or set it to `0` to force every re-read.

Bulk outreach activates one primary person per account and defaults to seven reviewed touches.
Discovery-ready accounts are held for research unless the request is a single manual routing note;
only action-ready evidence can produce a multi-touch campaign. An explicit four-touch request still
uses the compact email/email/LinkedIn/email cadence. Run `cargo run -- eval-outreach --double-blind`
to compare copy against the pairwise corpus before promoting a prompt or model change.
Angle selection and copy realization are separate calls by default. Set
`SPRUCE_FOLD_OUTREACH_PLANNER=1` only for a measured lower-cost comparison.

## The doctrine model

The old "invent a `$1M/year` figure" move is gone. Each account separates what can be **stated
as fact** (`observed_facts`) from what can only be **guessed** (`inferences`), plus the
commercial **hypothesis** being tested, its **mechanism**, a **measurable consequence** (never a
dollar figure), the one narrow **system concept** to offer, the **hard buyer question**, and the
**kill condition**. Contacts are chosen by **vantage point** (what they can observe/decide/route),
not seniority. Every sequence gets deterministic sendability checks and structured review. The
normal path creates an independent angle plan for the primary contact, realizes the copy in a
separate call, runs deterministic checks, then uses a separate final verifier. Verifier feedback can trigger
up to two stage-scoped rewrites; rejected copy never becomes an active draft. The older ten-lens
sales council remains available for deliberate audits with `SPRUCE_SALES_COUNCIL=1`, but is off on
the normal API path because it duplicates the independent verifier at much higher token cost.

The doctrine itself lives in editable **`playbooks/*.toml`** — a shared spine plus one file per
brand (`gnk`, `wapahki`, `outagehub`) — so you can tune voice, length, forbidden phrases, and
vantage notes without recompiling. Specialist roles are also editable rather than embedded in
Rust: **`playbooks/personas/planner.md`**, **`writer.md`**, and **`reviewer.md`**. The council's
source-grounded critic lenses live under **`playbooks/personas/critics/`**, one editable Markdown
file per critic.

## Knowledge ("SecondBrain")

Ingest business/sales books once; spruce-leaf parses them, distills each into compact **principle
cards** (via the selected backend), and retrieves only the few relevant to each pipeline stage
(BM25, no extra keys). Planning, writing, editing, and verification receive compact role-specific
context; the full book doctrine is not repeated inside every short-copy call. Retrieved cards favor
different source books before repeating one source, so a single title cannot dominate. Applied
principle IDs are stored on the sequence when a retrieved principle materially changes the work.
Model and reasoning lanes for writing, editing, and verification are independently configurable.

```sh
spruce-leaf ingest ./books                 # .txt / .md / .pdf, file or directory
spruce-leaf ingest book.pdf --max-sections 24
spruce-leaf ingest ./books --no-distill    # add searchable passages without model usage
# Re-run without --no-distill later to upgrade raw-only sources in place.
scripts/distill-core-library.sh            # retry the private core library safely
```

Markdown skill repositories use the same knowledge path. Install or clone them, then ingest their
skill directories with `--no-distill` for immediate passage retrieval; re-run later without the
flag to distill reusable principle cards.

The YouTube workflow builds copyright-safe research notes from official captions: it keeps
paraphrased summaries, timestamped principles, claims-to-verify, and source URLs, then deletes the
temporary caption files. The local manifest currently contains 14 core and 8 extended
sales/GTM interviews.

```sh
# Build or resume the 14 core episode notes, then merge them into the library.
YOUTUBE_NOTES_TIER=core scripts/build-youtube-notes.sh
scripts/ingest-youtube-notes.sh

# Optionally process the 8 extended sources and merge again (IDs are deduplicated).
YOUTUBE_NOTES_TIER=extended scripts/build-youtube-notes.sh
scripts/ingest-youtube-notes.sh
```

Episode metadata lives in `knowledge-sources/youtube-episodes.tsv`. Generated notes and their
catalog live under `.spruce/videos/`; full transcripts are never retained.

## AI-native GTM intelligence

Spruce Leaf follows the pattern visible in current AI-company hiring: agents maintain context,
research signals, CRM records, and draft candidates; a human owns discovery, trust, interpretation,
and commitments; technical proof begins only after confirmed pain and feeds its learning back into
the targeting system. The dated source memo is in `docs/modern-ai-gtm.md`.

Official-site research now checks a company's careers or jobs page as a best-effort source. A live
first-party role may support a 30-day `account.job_posting_workflow_evidence` signal when it explicitly
names a relevant system, workflow, responsibility, or investment. It can never prove pain, urgency,
budget, buying intent, or that the selected recipient owns the workflow. Set `SPRUCE_JOB_SIGNALS=0`
to skip the extra careers-page read when latency matters more than the additional evidence.

## Setup

Needs the Rust toolchain (`cargo`) and an OpenAI API key. Claude Code, Codex CLI, and Grok CLI are
optional local-backend alternatives.

Install the command once, then use `spruce-leaf` from the project directory. The installed
command asks Cargo for the latest local workspace build on every launch; Cargo's incremental
cache makes unchanged starts fast and automatically downloads any missing Rust dependencies.

```sh
cargo install --path . --force   # one-time command install
# Add OPENAI_API_KEY to your existing .env
spruce-leaf                      # latest local build + REPL + CRM (GPT-5.6 Terra)
spruce-leaf --model gpt-5.6-sol  # opt into frontier quality for a hard run
spruce-leaf --backend claude
spruce-leaf --backend codex
spruce-leaf --backend grok
# In the REPL: /model gpt   or   /model openai gpt-5.6-sol
```

CRM startup is automatic. An interactive `spruce-leaf` session starts a CRM owned by the current
build and in-memory runtime, choosing port 8787 or the next free loopback port. Standalone
`spruce-leaf crm` / `spruce-leaf gtm` viewers may reuse a protocol-compatible CRM that is already
running. The exact `http://127.0.0.1:<port>` URL is printed in the session header and `/crm` opens
it. `--port <number>` sets the first port to try; an occupied port no longer prevents startup.

The same localhost app has three surfaces:

- **Pipeline** (`/`, `/b/<brand>`) — the live people sheet, sequences, and execution state.
- **Strategy** (`/strategy`, `/strategy/<brand>`) — the business side of the SDR: what each brand
  is trying to accomplish, enabled motions, hard constraints, and the outreach doctrine from
  `businesses/*.toml` + `playbooks/*.toml`. It also shows the three agent personas, all ten sales
  council lenses, and the live book/skill/passages counts. `spruce-leaf crm` opens the Strategy
  board first.
- **GTM Lab** (`/gtm`, `/gtm/<brand>`) — the GTM-engineering control plane: canonical signals,
  versioned plays, account-level root-cause assessments, controlled experiments, attributed
  outcomes, and approval-gated customer proofs. Run `spruce-leaf gtm` or use `/gtm` in the REPL.

At phone widths the Pipeline becomes account → contact → touch cards instead of a compressed
spreadsheet. Approvals, LinkedIn connection state, full copy, QA, and delivery timing remain
available; Mac widths retain the dense people sheet. The localhost UI is still private and
single-user. See [ARCHITECTURE.md](ARCHITECTURE.md) for the authenticated mobile agent, Today
queue, cloud runtime, and outcome-oriented product plan.

For real execution, add `APOLLO_API_KEY` and one or more brand-prefixed mailboxes to `.env`:

```dotenv
APOLLO_API_KEY=...
# Required only for `enrich --phone`; Apollo delivers phone results asynchronously.
APOLLO_WEBHOOK_URL=https://your-webhook.example/apollo
GNK_FROM_NAME=Andrew
GNK_FROM_EMAIL=andrew@example.com
GNK_SMTP_HOST=smtp.example.com
GNK_SMTP_PORT=587
GNK_SMTP_USER=andrew@example.com
GNK_SMTP_PASS=...
GNK_IMAP_HOST=imap.example.com
GNK_IMAP_PORT=993
GNK_DAILY_CAP=30
COMPLIANCE_ADDRESS=123 Example Street, London, UK
```

Use the corresponding `WAPAHKI_` or `OUTAGEHUB_` prefix for other brands.
Mailbox caps are a second deliverability guardrail. The hard business-wide cap,
IANA timezones, recipient-local windows, and named industry/title timing rules
live under `[calendar]` in `businesses/<brand>.toml`; adding a mailbox never
multiplies the business cap.

## Three businesses, one CLI

The CLI loads an operating profile and an outreach playbook for the active `--brand` (or
`/brand` in the REPL). They are deliberately separate:

- `businesses/gnk.toml` — GnK's sales motion, goals, known facts, and unknowns.
- `businesses/wapahki.toml` — Wapahki's sales and partnership motions.
- `businesses/outagehub.toml` — OutageHub's sales, partnership, and funding motions, including
  official sources, fit criteria, grant-contact roles, project shapes, and funding-email doctrine.
- `playbooks/<brand>.toml` — voice, signature, copy limits, and prohibited language.

The interactive agent receives the active profile on every turn. A brand switch changes both
business context and outreach doctrine, and fails if either profile is missing.

### OutageHub funding workflow

Opportunity discovery is source-backed and restart-safe. It checks every source configured in
`businesses/outagehub.toml`, interleaves candidates so one catalog cannot crowd out the rest,
deduplicates by business/funder/programme/source, and records the official page, snapshot,
instrument type, deadline evidence, eligibility, blockers, unknowns, fit score, next action, and
last verification time. Closed recurring programmes and forecast calls remain on the watchlist;
rerun discovery to refresh them. Add `JINA_API_KEY` for the broad official-domain search source.

Business profiles may also carry source-linked `[[discovery_evidence]]` records. These are
first-party founder call learnings, kept separate from target-account facts. They shape ICP,
qualification, next questions, and at most one explicitly attributed follow-up angle; participant
estimates and cross-industry analogies cannot silently become claims about a prospect. Wapahki's
two current call records are visible on its Strategy page and mirrored in
`knowledge-sources/wapahki-discovery-calls.md`.

```sh
# Find/refresh official opportunities. "Grant" is not used as a catch-all:
# contributions, tax credits, procurement, pilots, challenges, and advisory programmes stay typed.
cargo run -- --brand outagehub discover-opportunities --limit 20
cargo run -- --brand outagehub opportunities
cargo run -- --brand outagehub opportunities --actionable

# Persist the published programme route, then map likely programme/innovation people in Apollo.
# People search is free in Apollo; organization fallback and --enrich can consume credits.
cargo run -- --brand outagehub opportunity-contacts <opportunity-id> --limit 3 --enrich

# Draft at most three pre-application enquiries. They ask a bounded fit question and never claim
# eligibility or ask a contact to award money. Drafts remain approval-gated by default.
cargo run -- --brand outagehub plan-funding-outreach <opportunity-id> --touches 2
cargo run -- --brand outagehub approve-funding --contact <contact-id>

# Produce a go/no-go brief with missing evidence, documents, budget questions, risks, and workplan.
cargo run -- --brand outagehub prepare-application <opportunity-id>

# Preview is read-only; live delivery still requires a healthy OUTAGEHUB_ mailbox and address.
cargo run -- daemon
cargo run -- daemon --live
cargo run -- inbox
```

Discovery and application drafting never fabricate incorporation, headcount, revenue, project
metrics, partners, matching funds, TRL, emissions impact, or eligibility. Unknown mandatory facts
produce `needs_information`; explicit mismatches produce `ineligible`, which blocks drafting.

## Using it

```
$ cargo run
╭────────────────────────────────────────────────────────╮
│ >_ Spruce Leaf (v0.1.0)                                │
│                                                        │
│ model:     claude · default                            │
│ brand:     gnk   /brand to change                      │
│ directory: /work/sales-os2                             │
│ crm:       http://localhost:8787   /crm to open        │
╰────────────────────────────────────────────────────────╯

  claude default · gnk · sales-os2
› find 5 logistics accounts with manual invoice reconciliation
• I’ll map the account pattern, then find the people closest to the workflow.

• Sourcing companies
  └ GnK · 5 account target · map up to 5 people each · activate 1 primary · active GTM play

  ✓ Built ICP
    └ 9 keywords · 8 buyer titles · 3 size bands

  ✓ Found candidate companies
    └ 10 returned · 10 new to qualify · 0 already judged

  ⠹ Qualifying root-cause fit
    ├ 6/10 reviewed · 2 qualified · 2 research-needed · 2 skipped
    └ Latest: research needed Acme Logistics · fit 61/100

  └ ⠹ 8/10 calls · 14.2k in · 2.1k out · 94s
```

The terminal keeps the transcript readable while work is live: private router scratch-work stays
private, one concise action intent appears before execution, and longer jobs redraw progress in
place as stable Codex-style milestones with blue active work, gray nested evidence, orange warnings,
green completions, elapsed time, model calls, tokens, and cost. Sourcing keeps ICP construction,
Apollo retrieval, official-site research, root-cause qualification, targeting learning, and contact
mapping on separate rows instead of flattening them into one spinner. Outreach gets a colored per-recipient tree
for account strategy/writing, per-recipient copy QA, and ready/rejected states; recipient reviews run
concurrently. The composer supports history, paste, an empty-state hint, and
Tab completion for slash commands.

Qualification distinguishes negative evidence from missing evidence. Accounts that fully satisfy
the active play are `qualified`; plausible accounts with partial support and no hard blocker are
retained as `research_needed`, but bulk multi-touch drafting holds them instead of turning missing
evidence into copy. A single manual routing note remains possible. Affirmative mismatches are
rejected. Each pass also persists recurring ICP and
contact-coverage failure patterns and injects them into the next ICP build; legacy two-signal rejects
from the older hard gate are automatically reconsidered once.

Outreach also streams into the CRM. Each recipient gets non-sendable `writing` placeholders as
soon as work begins; raw copy replaces them when the writer returns, reviewed copy replaces that
during QA, and the pipeline refreshes every three seconds. Only final-gate-approved copy is
promoted to an active draft sequence, so a failed rewrite cannot delete the prior unsent draft.

### Gmail login (per brand, browser OAuth)

Connect each brand mailbox without App Passwords or IMAP settings changes:

```sh
# One-time: put a Google Cloud Desktop OAuth client in .env
#   GOOGLE_CLIENT_ID=...
#   GOOGLE_CLIENT_SECRET=...
# Enable Gmail API on that project. Redirect: loopback http://127.0.0.1

spruce-leaf                # or just run the REPL
› /login gnk               # Chrome opens → sign in as the GnK mailbox
› /login wapahki
› /login outagehub
› /mail                    # status of linked accounts
› /mail-sync               # pull inbox + sent → conversations + learnings
```

Tokens live in `.spruce/google/<brand>.json` (owner-only permissions). Sync matches
threads to CRM people when possible, records reply / no-reply learnings, and is
what the daemon/inbox path uses when a brand is OAuth-linked.

### Reuse-first full motion

When you ask Spruce Leaf to find companies, the requested count is the **output count**, not an
instruction to accept the first Apollo results. The active versioned play first shapes the Apollo
ICP; Spruce Leaf then over-fetches a bounded candidate pool and requires source-backed evidence for
the operational decision, underlying root cause/current workaround, a reachable workflow vantage,
and a bounded proof. Generic industry, scale, hiring, or technology similarity cannot qualify an
account by itself. Qualified companies are ranked by play-fit score and canonical signal evidence,
and each assessment is visible in GTM Lab.

For example, `find 5 companies for ohub` uses the active OutageHub play and returns the five
strongest qualified candidates it found—not simply five outage-sensitive companies. Drafting then
inherits the exact play version, evidence IDs, and experiment arm that selected each account.

When you already have companies and people on file (for example after an earlier OutageHub run),
asking for “5 companies with up to 5 mapped people and outreach” **does not re-search Apollo**.
Spruce Leaf:

1. picks the strongest on-file accounts/contacts for that brand,
2. refreshes why each company fits (hypothesis / mechanism / “why them” — no Apollo),
3. reveals emails only for contacts still missing a verified address,
4. activates the strongest workflow owner at each account and writes a four-touch sequence only
   when the evidence is action-ready.

Current-policy drafts that already passed review are preserved on an ordinary
retry. Say `rewrite`, `redraft`, `refine`, or `replace drafts` when you
intentionally want accepted unsent copy regenerated too.

Apollo runs only for an account shortfall, or when you explicitly ask for **new/fresh** companies
(`force_new`). That keeps re-runs cheap and keeps sequences aligned with the current business
profile and playbook.

A full motion treats the requested account count as a fulfillment contract, not the size of one
search batch. If a sourcing pass produces no qualified accounts, the qualification misses are
saved and the ICP is derived again before another Apollo pass. Accounts with insufficient contact
coverage, no verified workflow owner, weak evidence, or copy that cannot pass review are removed
from the working set and replaced. Rejected copy gets one additional whole-sequence rewrite using
the saved reviewer feedback before replacement. An account slot counts only when a complete,
reviewed current-policy sequence exists for the requested touch shape.

Full motion also interleaves the funnel instead of exhausting the inference budget upstream. Each
adaptive pass deeply researches a small fresh-candidate wave sized to the remaining shortfall, then
immediately refreshes, enriches, and drafts any usable account before searching again. Standalone
`source` commands retain their wider over-fetch because they have no downstream writing stage to
reserve capacity for.

The motion stops short only at an explicit execution boundary: two adaptive searches with no
previously unseen companies, a provider/authentication/credit or model-budget stop, or the
configurable safety ceilings. The defaults allow eight adaptive sourcing passes and four
replacement rounds per requested account (plus four); operators can change them with
`SPRUCE_FULL_MOTION_SOURCE_PASSES` and `SPRUCE_FULL_MOTION_ROUNDS`. The terminal summary reports
the filled/asked count and the exact boundary instead of presenting a failed batch as completed
work.

The working-set IDs are enforced during enrichment: a 5-account × 5-person mapping request reveals
those five selected contacts at each account, rather than the first 25 unenriched rows in database
order. Outreach then activates one primary person per account instead of launching five parallel
threads.
The same cardinality contract applies to scoped re-drafting. A request for 5 accounts × 2 people
cannot be silently reduced by a router-supplied total limit; Spruce Leaf resolves the ten visible
people first, reports the exact scope, and reveals email only for selected identities that are still
pending. If Apollo cannot verify someone, the final response names the per-account coverage gap
instead of pretending the requested batch ran. Add “without Apollo,” “no enrichment,” or
“verified only” to keep that scoped run credit-free.
If the reasoning provider reaches a session limit, bulk drafting stops after the already-running
batch, preserves exact per-recipient feedback, saves any copy that already passed, and reports
provider-stopped separately from copy-rejected.

Open the dashboard to see both the real execution funnel (leads, people, verification, scheduled
touches, mailbox capacity, replies, and recent activity) and research-only campaign hypotheses.

REPL commands: `/crm`, `/gtm`, `/brand [key]`, `/model [openai|codex|claude|grok] [id|default]`, `/clear`,
`/help`, `/quit`.

The selected reasoning provider automatically falls back when it reports an
exhausted usage allowance. The switch persists for later calls. Use `/model codex` or
`/model openai` to switch manually; add a model ID to override that provider's default, or use
`default` to clear its override.

## Subcommands & options

```sh
cargo run                                 # interactive REPL (default)
cargo run -- run "<thesis>" [--accounts 5] [--contacts 5] [--touches 7] [--report brief.md]
cargo run -- ingest <path...> [--no-distill] [--max-sections 24]
cargo run -- crm                          # just serve the dashboard
cargo run -- gtm                          # open the GTM engineering lab

# Real, restart-safe SDR execution:
cargo run -- source "mid-market 3PL invoice reconciliation" --accounts 10 --contacts 3
cargo run -- enrich --limit 50            # Apollo reveal + DNS verification
cargo run -- plan --touches 7             # writes drafts; email stays approval-gated
cargo run -- approve                      # schedules email drafts only
cargo run -- mailboxes                    # load env config + check SPF/DMARC/MX
cargo run -- daemon                       # one read-only preview pass, then exit
cargo run -- daemon --live                # requires address + healthy sending domains
cargo run -- daemon --live --autopilot    # also fill discovery funnel toward configured targets
cargo run -- inbox                        # resolve threads + create approval-gated reply drafts
cargo run -- approve-replies              # schedule reply-agent drafts
cargo run -- inbox --book                 # also book an explicitly accepted offered slot
cargo run -- meetings                     # inspect pending/booked meetings
cargo run -- book-meetings                # approve pending calendar insertions
cargo run -- jobs                         # queue + dead-letter health
cargo run -- synthesize                    # recurring-problem/convergence report
cargo run -- stats
cargo run -- --brand wapahki calendar     # policy, 7-day capacity, observed timing
cargo run -- suppress person@example.com

# global options (any subcommand):
#   --brand <gnk|wapahki|outagehub>   brand playbook            (default gnk)
#   --playbooks <dir>                 playbook TOML directory   (default playbooks)
#   --businesses <dir>                business TOML directory   (default businesses)
#   --backend <openai|claude|codex|grok> inference provider      (default openai)
#   --model <id>                      backend model override    (default: its default)
#   --no-critique                     use deterministic QA only; skip the semantic copy edit
#   --concurrency <N>                 concurrent model calls    (default 2)
#   --port <N>                        preferred CRM port; otherwise reuse/new free localhost port
#   --store <path>                    CRM JSON store            (default .spruce/crm.json)
#   --knowledge <path>                knowledge library JSON    (default .spruce/knowledge.json)
#   --db <path>                       execution SQLite db        (default .spruce/sales.db)
```

The REPL prints input, cached-input, cache-write, output, failed-attempt, and fallback usage
after every request. `/usage` shows the cumulative per-stage breakdown. The same
metadata (never prompts or model output) is appended to
`.spruce/model-usage.jsonl`. Bulk sourcing, research, opportunity discovery, and
outreach fail fast when a provider is exhausted; automatic cross-provider
fallback is reserved for interactive routing and replies. Provider subprocesses
run without coding tools, project rules, plugins, or browser integrations so an
inference call does not pay for a second agent workspace.

## Layout

- `src/main.rs` — CLI + subcommands; starts the runtime, CRM server, and REPL.
- `src/repl.rs` — the interactive `spruce-leaf ›` prompt.
- `src/agent.rs` — the streaming structured-router agent for research and real execution actions.
- `src/engine.rs` — Responses API inference plus optional Codex, Claude, and Grok CLI adapters.
- `src/pipeline.rs` — the research-only pipeline: accounts → contacts (by vantage) → sequence → copy edit.
- `src/prompts.rs` / `src/playbook.rs` — per-stage schemas, editable personas, and brand playbooks.
- `src/business.rs` / `src/opportunity.rs` — active-business context and generic sourced opportunity pursuit.
- `src/gtm.rs` — canonical signal taxonomy, versioned GTM plays, sourcing policy, and action context.
- `src/knowledge.rs` — the book library: ingest, distill, BM25 retrieval.
- `src/crm.rs` — unified axum dashboard for the JSON research store and SQLite execution db.
- `src/ui.rs` — terminal streaming/progress rendering.
- `src/report.rs` — optional standalone Markdown brief.

### SDR execution layer

`src/{db,apollo,sourcing,enrich,verify,outreach,cadence,mailbox,send,inbox,reply_agent,jobs,google_calendar,triage,compliance}.rs`
form the real execution spine: SQLite durability, Apollo identity data, DNS verification,
approval-gated scheduling, capped SMTP delivery, durable job leases, RFC-threaded reply handling,
guarded Google Calendar booking, suppression, and metrics.
The CLI, interactive agent, and dashboard all operate on this same database.

### Discovery autopilot and meetings

`daemon --live --autopilot` runs the deterministic supervisor above the existing execution
pipeline. It fills each brand from the top—source accounts, reveal/verify people, then draft
reviewed cadences—and persists every lease, retry, result, and dead-letter row in SQLite. Cold
drafts remain behind `approve`; the autopilot flag does not grant a new sending permission.

Inbound identity prefers RFC `In-Reply-To` and `References` over the sender address. A new person
introduced on CC therefore stays attached to the original account and thread. Human replies stop
the cold sequence and produce a separate conversational draft; `approve-replies` schedules it.

Google Calendar is optional. Configure the OAuth variables and per-brand calendar settings shown
in `.env.example`. The reply agent may offer only slots returned free by Calendar. A meeting is
booked only when the prospect accepts an exact slot that appeared in a reply actually sent; the
system rechecks FreeBusy immediately before creating the event and asks Google to notify the
attendee. The live autopilot daemon may complete that guarded booking automatically. Plain live
mode and one-shot `inbox` record it as pending unless `inbox --book` is explicit; use
`book-meetings` to approve pending inserts.

### Email and LinkedIn cadence

New plans default to seven touches: email on days 0 and 3, a personalized LinkedIn
connection request on day 5, then email or conditional LinkedIn/email follow-ups
on days 9, 13, 17, and 21. The arc is diagnostic, useful contribution, objection,
routing, and close rather than seven paraphrases. An explicit four-touch request
uses days 0, 3, 7, and 14. There are no call touches. Connection requests are short
and contain no pitch or meeting ask.

Spruce Leaf does not scrape LinkedIn connection state. The CRM therefore exposes
an explicit status beside each person. A conditional touch is held as a manual
LinkedIn DM when the person is marked connected; otherwise it remains an
approval-gated email fallback. Completing a connection-request task marks the
person requested, and every email follow-up keeps RFC thread headers.

### Outreach calendar intelligence

Every planned email, LinkedIn task, and legacy call receives a recipient-local calendar
slot. Used and reserved capacity is counted across all channels at a maximum of
30 touchpoints per business day for each of `gnk`, `wapahki`, and `outagehub`
independently. The live
daemon rechecks both the local window and the business-wide email-send cap just
before delivery, so late approvals and legacy rows are deferred safely.

Weekend timing is never inferred globally. It must match a named rule in the
business profile, such as Wapahki's Saturday plant-operations hypothesis or
OutageHub's continuously operated incident-response cohort. `calendar` reports
used/reserved capacity and attributes replies to the last preceding send by local
weekday, rule, industry, and title/vantage cohort. Until the configured minimum
sample is reached, the agent labels these rules as hypotheses rather than learned
best times.

## Caveat

The legacy `run`/research path produces **model-generated account and person hypotheses**. In the
execution path, Apollo supplies real organization/person identity fields, but workflow fit,
mechanism, and commercial consequences remain hypotheses. Verify all claims before outreach.
