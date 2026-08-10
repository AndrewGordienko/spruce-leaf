# Running spruce-leaf in the cloud

The goal: generated sequences send **on a schedule from an always-on box**, not
your laptop — and you can watch it happen. Almost all of this already exists in
the binary; this doc is about *where* it runs and *how* to prove it works before
it touches a real prospect.

## What actually runs

The default deployment runs two long-lived containers against the same
persistent SQLite volume:

- `spruce-leaf daemon --live --batch 90` sends approved work and polls replies.
- `spruce-leaf --port 8788 crm` serves the calendar and pipeline privately.

The daemon does two required loops and one optional loop:

| Loop | What it does | Cadence |
| --- | --- | --- |
| **Cadence** | Sends touches whose `due_at` has arrived, respecting timezone windows, per-mailbox caps, and warmup. | every `--interval` (default 60s) |
| **Inbox** | Polls each mailbox's IMAP for replies, detects bounces/opt-outs, and runs the reply agent (classify → draft → optionally book). | every `--interval` (min 30s) |
| **Autopilot (optional)** | Sources new accounts, enriches emails, and drafts cadences toward funnel targets when `--autopilot` is explicitly added. | every `--interval` (min 15s) |

State lives in `.spruce/sales.db` (SQLite). The default reasoning backend is the
OpenAI Responses API, so the host needs `OPENAI_API_KEY`. Approved cold
sequences do not need to be regenerated before sending; the model is used only
when the optional sourcing/drafting loop runs or when an inbound reply needs to
be classified and drafted.

Because it keeps a SQLite file, the host must be a **persistent VM with a
persistent disk** — not a serverless/ephemeral platform.
Any small always-on Linux VM with a persistent disk works. A basic Ubuntu
DigitalOcean Droplet or Hetzner Cloud VM is the least surprising setup because
the checked-in Docker Compose file works unchanged. A Fly Machine also works if
`.spruce` is mounted on a Fly Volume, but SQLite should remain single-machine.

---

## Step 1 — Fill in `.env`

Copy `.env.example` to `.env` and set, at minimum:

- `OPENAI_API_KEY=` — used for optional drafting and reply intelligence.
- One mailbox per active brand: `<BRAND>_FROM_EMAIL`, `<BRAND>_SMTP_HOST/PORT/USER/PASS`,
  `<BRAND>_IMAP_HOST/PORT`. The daemon relays through *your* SMTP provider, so
  deliverability/IP reputation is the relay's, not the VM's.
- `COMPLIANCE_ADDRESS=` — a physical mailing address. `--live` refuses to send
  without it (CAN-SPAM/CASL).
- `SPRUCE_SEND_ALLOWLIST=` — **leave commented for production.** You'll set it
  during testing (Step 3).

`--live` also health-checks each sending domain's SPF/DMARC/MX and refuses to
send from a mis-authed one, so get DNS right before going live.

## Step 2 — Deploy

### Option A — Docker (recommended)

Reproducible, auto-restarts on crash and reboot, SQLite persists in a named
volume. On the box, with the repo checked out and `.env` in place:

```bash
docker compose build
# staged rollout is Step 3 — do NOT skip straight to `up`
```

### Option B — bare metal (systemd)

See the header of [`systemd/spruce-leaf.service`](systemd/spruce-leaf.service)
for the install commands. Build with `cargo build --release` (needs
`pkg-config` + `libssl-dev` for the IMAP TLS dependency), then run
`systemctl enable --now spruce-leaf`.

---

## Step 3 — Staged rollout (this is the testing plan)

Never point a fresh live daemon at real prospects. Walk these three gates.

### Gate 1 — Dry run (no mail leaves the box)

```bash
docker compose run --rm spruce-leaf daemon      # one pass, prints what WOULD send
```

`daemon` without `--live` previews a single cadence pass and exits — it validates
scheduling, capacity, and selection with zero sends. Expect
`preview complete: N touch(es) would send`.

### Gate 2 — Closed-loop self-test (real send, only to you)

Prove the entire loop — schedule → send → reply → classify → draft — against a
mailbox you own, with a hard guarantee nothing reaches a real prospect.

You need: one fully-configured **sending mailbox** (SMTP+IMAP, in `.env`) and any
**second inbox you can read and reply from** (the "prospect" — a personal Gmail
is fine).

1. **Arm the guardrail.** In `.env`, set the allowlist to only the prospect
   address, then confirm it's active:

   ```bash
   # .env
   SPRUCE_SEND_ALLOWLIST=prospect@youraddress.com
   ```

   With this set, a live send to anything else is refused in the SMTP transport
   itself — impossible to bypass.

2. **Seed a verified test person** (no Apollo credits spent). Pick a timezone
   currently in business hours so it sends immediately:

   ```bash
   docker compose run --rm spruce-leaf \
     seed-test-lead --email prospect@youraddress.com --timezone America/Toronto
   ```

   It prints the exact `plan` and `daemon` commands with the new person id.

3. **Draft + schedule the sequence** for that person:

   ```bash
   docker compose run --rm spruce-leaf plan --auto --person <person_id>
   ```

4. **Run it live** and watch:

   ```bash
   docker compose up -d spruce-leaf crm
   docker compose logs -f          # look for "cadence sent 1 touch(es)"
   ```

   The email lands in the prospect inbox. **Reply to it as the prospect.** Within
   one `--interval`, the inbox loop ingests the reply and the reply agent drafts a
   response — check it:

   ```bash
   docker compose exec spruce-leaf spruce-leaf inbox   # or watch the logs
   docker compose exec spruce-leaf spruce-leaf approve-replies   # send the draft back
   ```

   To also test **meeting booking**, configure the brand's Google Calendar env
   and run with `--autopilot`; when the prospect accepts an offered slot, a real
   calendar event is created (`spruce-leaf meetings` to confirm).

### Gate 3 — Production

Once the loop works: **comment out `SPRUCE_SEND_ALLOWLIST`**, remove the test
lead if you like (it's harmless — it just has your address), and start the real
daemon and private calendar:

```bash
docker compose up -d
```

---

## Watching it ("we can see it send out")

- **Live feed:** `docker compose logs -f` — every pass prints `cadence sent N
  touch(es)` and inbox activity. This is the literal "watch it send."
- **Funnel + queue:** `docker compose exec spruce-leaf spruce-leaf stats` (leads
  → verified → contacted), `... jobs` (autopilot queue health), `... meetings`.
- **Audit trail:** every send/bounce/reply/booking is an append-only row in the
  `events` table in `.spruce/sales.db`.
- **Graphical calendar + CRM:** Compose binds the dashboard only to the VM's
  loopback interface. Forward it over SSH:
  `ssh -L 8788:127.0.0.1:8788 user@your-vm`, then open
  `http://127.0.0.1:8788`. Do not expose it to the public internet.

## What's automatic vs. gated

The "sales intelligence on a reply" is built and runs in the loop, but sending on
your behalf is deliberately gated:

- **Cold touches** planned with `--auto` (or approved via `approve`) send
  autonomously.
- **Replies** are ingested, classified, and *drafted* autonomously — but the
  draft waits for `approve-replies` before it goes out. Keep this gate until you
  trust the drafts; removing it is a future config flag, not a code gap.
- **Bookings** happen automatically only under `--autopilot`, and only when the
  prospect accepts a slot you previously offered (re-checked free first).

## Ongoing ops

- **API-key rotation:** update `OPENAI_API_KEY` in `.env`, then run
  `docker compose up -d` to recreate the services.
- **Backups:** the whole world is `.spruce/sales.db`. Snapshot the `spruce-data`
  volume (or the file) on a schedule. `.db-wal`/`.db-shm` are transient.
- **Restarts:** `restart: unless-stopped` (Docker) / `Restart=on-failure`
  (systemd) bring it back after crashes and reboots. In-flight state is durable
  in SQLite, so a restart resumes cleanly.

## Troubleshooting

- `refusing live sending: COMPLIANCE_ADDRESS is unset` → set it in `.env`.
- `refusing live sending: mailbox domain checks failed` → fix SPF/DMARC/MX on the
  sending domain.
- `recipient ... not in SPRUCE_SEND_ALLOWLIST` → the guardrail is still armed;
  comment it out for production (Gate 3).
- `claude /status` shows `API key` → unset `ANTHROPIC_API_KEY` so subscription
  auth is used.
- Nothing sends in the closed-loop test → the touch is scheduled for the next
  business-hours slot in its timezone; re-seed with a `--timezone` currently in
  business hours, or check `spruce-leaf calendar`.
