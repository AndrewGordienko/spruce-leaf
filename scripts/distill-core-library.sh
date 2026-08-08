#!/usr/bin/env bash
set -euo pipefail

# Upgrade the privately staged raw book/playbook sources with principle cards.
# Raw imports remain searchable before this succeeds, and reruns are safe: the
# importer skips completed sources and retries only sources with zero cards.

backend="${SPRUCE_DISTILL_BACKEND:-claude}"
max_sections="${SPRUCE_DISTILL_MAX_SECTIONS:-6}"
concurrency="${SPRUCE_DISTILL_CONCURRENCY:-2}"
binary="${SPRUCE_DISTILL_BINARY:-target/debug/spruce-leaf}"

if [[ ! -x "$binary" ]]; then
  cargo build
fi

sources=(
  .spruce/books/purchased/*.txt
  .spruce/books/founder-playbook/*.md
  .spruce/books/*official*.pdf
)

"$binary" \
  --backend "$backend" \
  --concurrency "$concurrency" \
  ingest "${sources[@]}" \
  --max-sections "$max_sections"
