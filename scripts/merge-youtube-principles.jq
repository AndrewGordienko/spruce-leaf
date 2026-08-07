def video_principles($episodes; $books):
  [
    $episodes[] as $episode
    | ($books[] | select(.source == $episode.note_source)) as $book
    | $episode.principles
    | to_entries[]
    | .value as $principle
    | {
        id: ("yt-" + $episode.video_id + "-p" + ((.key + 1) | tostring)),
        book_id: $book.id,
        book_title: $episode.title,
        name: $principle.name,
        summary: $principle.summary,
        when_to_use: (
          $principle.when_to_use
          + " Source: "
          + $episode.source_url
          + "&t="
          + ($principle.timestamp_seconds | tostring)
          + "s. Confidence: "
          + $principle.confidence
          + "."
        ),
        tags: ($principle.tags + [
          "youtube",
          ($episode.speaker | ascii_downcase | gsub("[^a-z0-9]+"; "-") | gsub("(^-|-$)"; ""))
        ] | unique),
        stages: ($principle.stages | unique)
      }
  ];

($episodes[0]) as $episode_catalog
| .books as $original_books
| video_principles($episode_catalog; $original_books) as $new_principles
| .principles = ((.principles + $new_principles) | unique_by(.id))
| .books = [
    .books[]
    | . as $book
    | ([ $episode_catalog[] | select(.note_source == $book.source) ][0]) as $episode
    | if $episode == null then .
      else .title = $episode.title | .author = $episode.speaker
      end
  ]
| .chunks = [
    .chunks[]
    | . as $chunk
    | ([ $episode_catalog[] | select(.note_source == ($original_books[] | select(.id == $chunk.book_id) | .source)) ][0]) as $episode
    | if $episode == null then . else .book_title = $episode.title end
  ]
| .principles as $all_principles
| .books = [
    .books[]
    | . as $book
    | .n_principles = ([ $all_principles[] | select(.book_id == $book.id) ] | length)
  ]
