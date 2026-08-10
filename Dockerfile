# syntax=docker/dockerfile:1
#
# spruce-leaf cloud image: the unattended outreach daemon and CRM. The default
# OpenAI Responses API backend reads OPENAI_API_KEY from the runtime environment.

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
# libssl3: native-tls (IMAP) at runtime. ca-certificates: reach
# SMTP/IMAP/Apollo/Google/OpenAI over TLS.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
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
CMD ["daemon", "--live", "--batch", "90"]
