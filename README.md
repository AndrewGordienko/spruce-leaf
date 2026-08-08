# spruce-leaf

A **"Codex for sales."** Launch it, type what you're hunting for, and it runs the whole
play: find accounts with an expensive workflow, the people positioned to see that workflow,
and a hypothesis-led outreach sequence for each — grounded in a library of real sales books —
then files everything in a local CRM.

It's a Rust CLI with a pluggable local-CLI reasoning engine. Each line you type goes to Codex
(the default) or Claude, which can research a campaign or operate the real execution pipeline:
Apollo sourcing, contact enrichment, verified outreach planning, approval, and funnel reporting.

**No model API key is needed.** Reasoning uses your existing authenticated `codex` or `claude`
CLI session. Select one with `--backend codex|claude`; `claude` is the default. Apollo and mailbox
credentials are still required for real sourcing and delivery.

## The doctrine model

The old "invent a `$1M/year` figure" move is gone. Each account separates what can be **stated
as fact** (`observed_facts`) from what can only be **guessed** (`inferences`), plus the
commercial **hypothesis** being tested, its **mechanism**, a **measurable consequence** (never a
dollar figure), the one narrow **system concept** to offer, the **hard buyer question**, and the
**kill condition**. Contacts are chosen by **vantage point** (what they can observe/decide/route),
not seniority. Every sequence gets deterministic sendability checks plus one structured LLM
**copy-edit pass** that reviews and corrects only failed touches in the same response.

The doctrine itself lives in editable **`playbooks/*.toml`** — a shared spine plus one file per
brand (`gnk`, `wapahki`, `outagehub`) — so you can tune voice, length, forbidden phrases, and
vantage notes without recompiling.

## Knowledge ("SecondBrain")

Ingest business/sales books once; spruce-leaf parses them, distills each into compact **principle
cards** (via the selected backend), and retrieves the handful relevant to each pipeline stage (BM25,
no extra keys) so outputs are grounded in — and cite — real playbooks instead of the model's unaided
priors. Strategy-producing calls also receive a compact, always-on twelve-book doctrine; retrieved
cards favor different source books before repeating one source, so a single title cannot dominate.

```sh
spruce-leaf ingest ./books                 # .txt / .md / .pdf, file or directory
spruce-leaf ingest book.pdf --max-sections 24
spruce-leaf ingest ./books --no-distill    # add searchable passages without model usage
# Re-run without --no-distill later to upgrade raw-only sources in place.
scripts/distill-core-library.sh            # retry the private core library safely
```

The YouTube workflow builds copyright-safe research notes from official captions: it keeps
paraphrased summaries, timestamped principles, claims-to-verify, and source URLs, then deletes the
temporary caption files. The tracked manifest currently contains 12 core and 8 extended
Hormozi/Serhant interviews.

```sh
# Build or resume the 12 core episode notes, then merge them into the library.
YOUTUBE_NOTES_TIER=core scripts/build-youtube-notes.sh
scripts/ingest-youtube-notes.sh

# Optionally process the 8 extended sources and merge again (IDs are deduplicated).
YOUTUBE_NOTES_TIER=extended scripts/build-youtube-notes.sh
scripts/ingest-youtube-notes.sh
```

Episode metadata lives in `knowledge-sources/youtube-episodes.tsv`. Generated notes and their
catalog live under `.spruce/videos/`; full transcripts are never retained.

## Setup

Needs the Rust toolchain (`cargo`) and either Claude Code (default) or the `codex` CLI on PATH.

Install the command once, then use `spruce-leaf` from the project directory. The installed
command asks Cargo for the latest local workspace build on every launch; Cargo's incremental
cache makes unchanged starts fast and automatically downloads any missing Rust dependencies.

```sh
cargo install --path . --force   # one-time command install
spruce-leaf                      # latest local build + REPL + CRM
spruce-leaf --backend codex
```

CRM startup is automatic. Spruce Leaf first reuses an existing Spruce Leaf CRM; otherwise it
tries port 8787 and moves to the next free loopback port. The exact `http://127.0.0.1:<port>` URL
is printed in the session header and `/crm` opens it. `--port <number>` sets a preferred port,
but an occupied port no longer prevents startup.

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

• Running campaign
  └ GnK · manual invoice reconciliation · 5×5×7
    ⠹ Researching accounts…
```

The terminal keeps the transcript readable while work is live: model reasoning streams as muted
commentary, actions become nested tool cells, and longer campaigns redraw account-level progress
in place with elapsed time, model calls, tokens, and cost. The composer supports history, paste,
an empty-state hint, and Tab completion for slash commands.

Open the dashboard to see both the real execution funnel (leads, people, verification, scheduled
touches, mailbox capacity, replies, and recent activity) and research-only campaign hypotheses.

REPL commands: `/crm`, `/brand [key]`, `/model [codex|claude] [id|default]`, `/clear`,
`/help`, `/quit`.

The selected reasoning CLI automatically falls back to the other provider when it reports an
exhausted usage allowance. The switch persists for later calls. Use `/model codex` or
`/model claude` to switch manually; add a model ID to override that provider's default, or use
`default` to clear its override.

## Subcommands & options

```sh
cargo run                                 # interactive REPL (default)
cargo run -- run "<thesis>" [--accounts 5] [--contacts 5] [--touches 7] [--report brief.md]
cargo run -- ingest <path...> [--no-distill] [--max-sections 24]
cargo run -- crm                          # just serve the dashboard

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
#   --backend <codex|claude>          reasoning CLI             (default claude)
#   --model <id>                      backend model override    (default: its default)
#   --no-critique                     use deterministic QA only; skip the semantic copy edit
#   --concurrency <N>                 concurrent model calls    (default 2)
#   --port <N>                        preferred CRM port; otherwise reuse/new free localhost port
#   --store <path>                    CRM JSON store            (default .spruce/crm.json)
#   --knowledge <path>                knowledge library JSON    (default .spruce/knowledge.json)
#   --db <path>                       execution SQLite db        (default .spruce/sales.db)
```

The REPL prints input, cached-input, output, failed-attempt, and fallback usage
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
- `src/engine.rs` — provider-neutral structured/streamed inference over Codex or Claude CLI.
- `src/pipeline.rs` — the research-only pipeline: accounts → contacts (by vantage) → sequence → copy edit.
- `src/prompts.rs` / `src/playbook.rs` — per-stage prompts/schemas and the brand playbooks.
- `src/business.rs` / `src/opportunity.rs` — active-business context and generic sourced opportunity pursuit.
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

### Outreach calendar intelligence

Every planned email, LinkedIn task, and call receives a recipient-local calendar
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
