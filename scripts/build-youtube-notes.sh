#!/usr/bin/env bash
set -euo pipefail
trap 'exit 130' INT TERM

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

manifest_path=${1:-knowledge-sources/youtube-episodes.tsv}
output_dir=${2:-.spruce/videos}
selected_tier=${YOUTUBE_NOTES_TIER:-core}
model_name=${YOUTUBE_NOTES_MODEL:-sonnet}
max_caption_chars=${YOUTUBE_NOTES_MAX_CAPTION_CHARS:-260000}
schema_path=knowledge-sources/youtube-note-schema.json

for required_command in yt-dlp jq claude; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "missing required command: $required_command" >&2
    exit 1
  fi
done

if [[ ! -f "$manifest_path" ]]; then
  echo "manifest not found: $manifest_path" >&2
  exit 1
fi

if [[ ! -f "$schema_path" ]]; then
  echo "schema not found: $schema_path" >&2
  exit 1
fi

notes_dir="$output_dir/notes"
principles_dir="$output_dir/principles"
mkdir -p "$notes_dir" "$principles_dir"

schema_json=$(jq -c . "$schema_path")
system_prompt='You convert official public YouTube captions into concise, original research notes for a B2B sales knowledge base. Paraphrase; do not reproduce the transcript and do not quote the speakers. Extract durable, specific operating principles that can change account selection, stakeholder selection, discovery, offer design, objection handling, negotiation, or outreach. Do not turn a personal anecdote into a universal law. Treat revenue, conversion, growth, and case-study numbers as speaker claims that need independent verification. Prefer fewer high-confidence principles over filler. Timestamps must point near the supporting discussion in the supplied timed captions.'

render_note() {
  local principle_path=$1
  local note_path=$2
  local video_url=$3

  jq -r --arg source_url "$video_url" '
    def timestamp:
      (. / 3600 | floor) as $h
      | ((. % 3600) / 60 | floor) as $m
      | (. % 60) as $s
      | if $h > 0
        then "\($h):\($m|tostring|if length == 1 then "0" + . else . end):\($s|tostring|if length == 1 then "0" + . else . end)"
        else "\($m):\($s|tostring|if length == 1 then "0" + . else . end)"
        end;
    "# " + .title,
    "",
    "- Speaker: " + .speaker,
    "- Channel: " + .channel,
    "- Published: " + .upload_date,
    "- Duration: " + (.duration_seconds | timestamp),
    "- Source: " + .source_url,
    "- Source basis: derived summary of official YouTube captions; no transcript retained",
    "- Focus: " + .focus,
    "",
    "## Summary",
    "",
    .summary,
    "",
    "## Topics",
    "",
    (.topics | map("- " + .) | join("\n")),
    "",
    "## Actionable principles",
    "",
    (.principles | map(
      "### " + .name + "\n\n"
      + .summary + "\n\n"
      + "- When to use: " + .when_to_use + "\n"
      + "- Pipeline stages: " + (.stages | join(", ")) + "\n"
      + "- Tags: " + (.tags | join(", ")) + "\n"
      + "- Confidence: " + .confidence + "\n"
      + "- Source timestamp: [" + (.timestamp_seconds | timestamp) + "](" + $source_url + "&t=" + (.timestamp_seconds | tostring) + "s)"
    ) | join("\n\n")),
    "",
    "## Speaker claims to verify",
    "",
    (if (.claims_to_verify | length) == 0
     then "- None captured."
     else (.claims_to_verify | map("- " + .) | join("\n"))
     end)
  ' "$principle_path" > "$note_path"
}

processed=0
skipped=0
failed=0

while IFS=$'\t' read -r video_id speaker tier focus; do
  [[ -z "$video_id" || "$video_id" == \#* ]] && continue
  [[ "$selected_tier" != "all" && "$tier" != "$selected_tier" ]] && continue

  note_path="$notes_dir/$video_id.md"
  principle_path="$principles_dir/$video_id.json"
  if [[ -s "$note_path" && -s "$principle_path" ]]; then
    echo "skip $video_id (note already exists)"
    skipped=$((skipped + 1))
    continue
  fi
  if [[ -s "$principle_path" && ! -s "$note_path" ]]; then
    echo "repair $video_id (rendering note from saved principles)"
    render_note "$principle_path" "$note_path" "https://www.youtube.com/watch?v=$video_id"
    skipped=$((skipped + 1))
    continue
  fi

  echo "summarize $video_id — $speaker"
  video_url="https://www.youtube.com/watch?v=$video_id"

  if (
    episode_tmp=$(mktemp -d "${TMPDIR:-/tmp}/spruce-youtube.XXXXXX")
    trap 'rm -rf -- "$episode_tmp"' EXIT

    metadata_path="$episode_tmp/metadata.json"
    result_path="$episode_tmp/result.json"
    transcript_path="$episode_tmp/timed-captions.txt"

    yt-dlp --skip-download --no-warnings --dump-single-json "$video_url" > "$metadata_path"

    caption_lang=$(jq -r '
      if ((.subtitles["en-orig"] // []) | length) > 0 then "en-orig"
      elif ((.subtitles.en // []) | length) > 0 then "en"
      elif ((.automatic_captions["en-orig"] // []) | length) > 0 then "en-orig"
      elif ((.automatic_captions.en // []) | length) > 0 then "en"
      else empty
      end
    ' "$metadata_path")

    if [[ -z "$caption_lang" ]]; then
      echo "no English captions available for $video_id" >&2
      exit 2
    fi

    yt-dlp \
      --skip-download \
      --no-warnings \
      --write-subs \
      --write-auto-subs \
      --sub-langs "$caption_lang" \
      --sub-format json3 \
      -o "$episode_tmp/%(id)s.%(ext)s" \
      "$video_url" >/dev/null

    caption_path=$(find "$episode_tmp" -maxdepth 1 -type f -name "$video_id.*.json3" -print -quit)
    if [[ -z "$caption_path" ]]; then
      echo "caption download did not produce json3 for $video_id" >&2
      exit 3
    fi

    jq -r '
      .events[]
      | select(((.segs // []) | length) > 0)
      | ([.segs[].utf8] | join("") | gsub("[\\n\\r]+"; " ")) as $text
      | select(($text | gsub("[[:space:]]+"; "") | length) > 0)
      | (((.tStartMs // 0) / 1000) | floor | tostring) + "\t" + $text
    ' "$caption_path" > "$transcript_path"

    coverage_note="full timed captions"
    caption_chars=$(wc -c < "$transcript_path" | tr -d ' ')
    if (( caption_chars > max_caption_chars )); then
      sample_stride=$(((caption_chars + max_caption_chars - 1) / max_caption_chars))
      compacted_path="$episode_tmp/compacted-captions.txt"
      awk -F $'\t' -v stride="$sample_stride" '
        int(($1 + 0) / 60) % stride == 0 { print }
      ' "$transcript_path" > "$compacted_path"
      mv "$compacted_path" "$transcript_path"
      coverage_note="even timeline sample: retained one minute out of every $sample_stride minutes because the full caption text exceeded the model context budget"
    fi

    {
      echo "SOURCE METADATA"
      jq -r '
        "Title: \(.title)\nChannel: \(.channel)\nPublished: \(.upload_date)\nDuration seconds: \(.duration)\nURL: \(.webpage_url)"
      ' "$metadata_path"
      echo "Speaker: $speaker"
      echo "Requested focus: $focus"
      echo "Caption coverage: $coverage_note"
      echo
      echo "TIMED CAPTIONS"
      echo "Each line begins with seconds from the start. Produce only the requested structured notes."
      cat "$transcript_path"
    } | claude \
      -p \
      --model "$model_name" \
      --system-prompt "$system_prompt" \
      --json-schema "$schema_json" \
      --output-format json > "$result_path"

    if [[ $(jq -r '.is_error // false' "$result_path") == "true" ]]; then
      jq -r '.result // "Claude returned an unknown error"' "$result_path" >&2
      exit 4
    fi

    jq \
      --arg video_id "$video_id" \
      --arg speaker "$speaker" \
      --arg focus "$focus" \
      --arg tier "$tier" \
      --arg note_source "$note_path" \
      --slurpfile metadata "$metadata_path" '
        .structured_output
        + {
            video_id: $video_id,
            speaker: $speaker,
            focus: $focus,
            tier: $tier,
            note_source: $note_source,
            title: $metadata[0].title,
            channel: $metadata[0].channel,
            upload_date: $metadata[0].upload_date,
            duration_seconds: $metadata[0].duration,
            source_url: $metadata[0].webpage_url,
            source_kind: "official-youtube-captions-derived-summary"
          }
      ' "$result_path" > "$principle_path"

    render_note "$principle_path" "$note_path" "$video_url"
  ); then
    processed=$((processed + 1))
  else
    echo "failed $video_id" >&2
    failed=$((failed + 1))
  fi
done < "$manifest_path"

find "$principles_dir" -maxdepth 1 -type f -name '*.json' -print0 \
  | sort -z \
  | xargs -0 jq -s '.' > "$output_dir/catalog.json"

echo "youtube notes: $processed processed, $skipped skipped, $failed failed"
echo "catalog: $output_dir/catalog.json"

if (( failed > 0 )); then
  exit 1
fi
