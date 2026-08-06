# spruce-leaf

A **"Codex for sales."** Launch it, type what you're hunting for, and it runs the whole
play: find accounts with an expensive workflow, the people positioned to see that workflow,
and a hypothesis-led outreach sequence for each — grounded in a library of real sales books —
then files everything in a local CRM.

It's a Rust CLI with a Claude "underbase": each line you type goes to Claude, which decides
whether to run a campaign, search the book library, list the CRM, or just reply.

**No API key needed.** Reasoning runs through your local `claude` CLI (Claude Code) —
spruce-leaf shells out to `claude -p --output-format json --json-schema …` and uses your
existing Claude authentication. Each headless call re-primes Claude Code's context, so a full
campaign takes a minute or two and costs a few dollars of usage; pass `--model sonnet` or lower
the counts to trim that.

## The doctrine model

The old "invent a `$1M/year` figure" move is gone. Each account separates what can be **stated
as fact** (`observed_facts`) from what can only be **guessed** (`inferences`), plus the
commercial **hypothesis** being tested, its **mechanism**, a **measurable consequence** (never a
dollar figure), the one narrow **system concept** to offer, the **hard buyer question**, and the
**kill condition**. Contacts are chosen by **vantage point** (what they can observe/decide/route),
not seniority. Every sequence gets a mechanical lint (forbidden phrases + length band) and an LLM
**pre-send critique** that rewrites each touch to pass.

The doctrine itself lives in editable **`playbooks/*.toml`** — a shared spine plus one file per
brand (`gnk`, `wapahki`, `outagehub`) — so you can tune voice, length, forbidden phrases, and
vantage notes without recompiling.

## Knowledge ("SecondBrain")

Ingest business/sales books once; spruce-leaf parses them, distills each into compact **principle
cards** (via Claude), and retrieves the handful relevant to each pipeline stage (BM25, no extra
keys) so outputs are grounded in — and cite — real playbooks instead of the model's unaided priors.

```sh
spruce-leaf ingest ./books                 # .txt / .md / .pdf, file or directory
spruce-leaf ingest book.pdf --max-sections 24
```

## Setup

Needs the Rust toolchain (`cargo`) and the `claude` CLI on your PATH (`claude --version`).

```sh
cargo run                 # launches the interactive REPL + CRM
```

## Using it

```
$ cargo run
🌲 spruce-leaf — Codex for sales
   CRM dashboard: http://localhost:8787
   brand: gnk   (switch with /brand <gnk | wapahki | outagehub>)
spruce-leaf › find 5 accounts drowning in manual invoice reconciliation in
              mid-market logistics, 5 people each, 7 touches
  → run_campaign [GnK]: "..."  (5×5×7, ~55 claude calls, a minute or two)
  ...
Filed 5 accounts, 25 contacts, and 175 touches into the CRM — view at http://localhost:8787
```

Open the dashboard (`/crm`) to browse accounts → contacts → touches and mark each sent.

REPL commands: `/crm`, `/brand [key]`, `/clear`, `/help`, `/quit`.

## Subcommands & options

```sh
cargo run                                 # interactive REPL (default)
cargo run -- run "<thesis>" [--accounts 5] [--contacts 5] [--touches 7] [--report brief.md]
cargo run -- ingest <path...> [--no-distill] [--max-sections 24]
cargo run -- crm                          # just serve the dashboard

# global options (any subcommand):
#   --brand <gnk|wapahki|outagehub>   brand playbook            (default gnk)
#   --playbooks <dir>                 playbook TOML directory   (default playbooks)
#   --model <id>                      claude CLI model          (default: its default)
#   --no-critique                     skip the pre-send critique/rewrite
#   --concurrency <N>                 concurrent claude calls   (default 5)
#   --port <N>                        CRM dashboard port        (default 8787)
#   --store <path>                    CRM JSON store            (default .spruce/crm.json)
#   --knowledge <path>                knowledge library JSON    (default .spruce/knowledge.json)
```

## Layout

- `src/main.rs` — CLI + subcommands; starts the runtime, CRM server, and REPL.
- `src/repl.rs` — the interactive `spruce-leaf ›` prompt.
- `src/agent.rs` — the streaming structured-router agent (run_campaign / search_knowledge / …).
- `src/engine.rs` — the Claude underbase: `structured()` / `structured_streamed()` over the `claude` CLI, with token/cost `Stats`.
- `src/pipeline.rs` — the doctrine pipeline: accounts → contacts (by vantage) → sequence → critique.
- `src/prompts.rs` / `src/playbook.rs` — per-stage prompts/schemas and the brand playbooks.
- `src/knowledge.rs` — the book library: ingest, distill, BM25 retrieval.
- `src/crm.rs` — JSON-backed store + axum web dashboard.
- `src/ui.rs` — terminal streaming/progress rendering.
- `src/report.rs` — optional standalone Markdown brief.

### SDR execution layer (in progress)

`src/{db,apollo,enrich,verify,sourcing,mailbox,send,compliance}.rs` are the beginnings of a real
execution spine — SQLite durability, Apollo enrichment, email verification (MX + SPF/DKIM/DMARC),
SMTP sending, and reply parsing. These compile but are **not yet wired** into the pipeline/agent.

## Caveat

Accounts, people, and every claim are **model-generated hypotheses** — only the `observed_facts`
are meant to be stated as fact. Verify against real data before any outreach.
