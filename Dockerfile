# syntax=docker/dockerfile:1
#
# spruce-leaf cloud image: the outreach daemon plus the Claude Code CLI it shells
# out to for reasoning. Runs unattended — no browser, no API key. Authenticate
# the subscription with `claude setup-token` and pass CLAUDE_CODE_OAUTH_TOKEN in.

########## builder ##########
FROM rust:1-bookworm AS builder
WORKDIR /build
# Everything is rustls EXCEPT IMAP (native-tls), which needs OpenSSL at build time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
# Compile dependencies against a stub main first so they cache across code edits.
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs \
    && cargo build --release \
    && rm -rf src
COPY . .
# Bust the stale stub-built binary so the real sources are compiled.
RUN touch src/main.rs && cargo build --release

########## runtime ##########
FROM debian:bookworm-slim AS runtime
# libssl3: native-tls (IMAP) at runtime. curl + ca-certificates: fetch & run the
# claude CLI and reach SMTP/IMAP/Apollo/Google/Anthropic over TLS.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 curl \
    && rm -rf /var/lib/apt/lists/*
# Claude Code CLI — native binary, no Node runtime required. Symlink onto PATH so
# the engine's `Command::new("claude")` resolves it.
RUN curl -fsSL https://claude.ai/install.sh | bash \
    && ln -sf /root/.local/bin/claude /usr/local/bin/claude \
    && claude --version
WORKDIR /app
# Brand doctrine + operating context the daemon reads at startup. Baked in.
COPY playbooks ./playbooks
COPY businesses ./businesses
COPY --from=builder /build/target/release/spruce-leaf /usr/local/bin/spruce-leaf
# SQLite + usage ledger live here; mount a named volume so state survives restarts.
VOLUME ["/app/.spruce"]
ENTRYPOINT ["spruce-leaf"]
# Steady-state intent. Complete the staged rollout in deploy/README.md BEFORE the
# first `up`: dry-run, then an allowlisted closed-loop test, THEN production.
CMD ["daemon", "--live", "--autopilot"]
