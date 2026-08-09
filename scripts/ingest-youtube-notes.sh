#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

spruce_bin=${SPRUCE_BIN:-target/debug/spruce-leaf}
knowledge_path=${SPRUCE_KNOWLEDGE:-.spruce/knowledge.json}
notes_dir=${YOUTUBE_NOTES_DIR:-.spruce/videos/notes}
catalog_path=${YOUTUBE_CATALOG:-.spruce/videos/catalog.json}
merge_filter=scripts/merge-youtube-principles.jq
candidate_path="${knowledge_path}.youtube-merge.tmp"

for required_file in "$spruce_bin" "$knowledge_path" "$catalog_path" "$merge_filter"; do
  if [[ ! -f "$required_file" ]]; then
    echo "required file not found: $required_file" >&2
    exit 1
  fi
done

"$spruce_bin" \
  --backend openai \
  --knowledge "$knowledge_path" \
  ingest "$notes_dir" \
  --no-distill

jq \
  --slurpfile episodes "$catalog_path" \
  -f "$merge_filter" \
  "$knowledge_path" > "$candidate_path"

jq empty "$candidate_path"
mv "$candidate_path" "$knowledge_path"

jq -r '
  "youtube knowledge merged: "
  + (([.books[] | select(.source | startswith(".spruce/videos/notes/"))] | length) | tostring)
  + " episodes, "
  + (([.principles[] | select(.id | startswith("yt-"))] | length) | tostring)
  + " video principles; library total: "
  + ((.books | length) | tostring)
  + " sources, "
  + ((.principles | length) | tostring)
  + " principles, "
  + ((.chunks | length) | tostring)
  + " passages."
' "$knowledge_path"
