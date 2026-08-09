# Outreach validation corpus

`outreach-gold.jsonl` contains blinded, human-labelled comparisons used to
evaluate outreach judgment. Human-approved messages are reference outcomes,
not production templates: their wording, companies, contacts, facts, numbers,
and hypotheses must never be injected into a different prospect's prompt.

Prompts and review rules may learn the general qualities the comparisons
reward: a recognizable operating moment, a plausible consequence, ordinary
language, a concrete and limited seller mechanism, role relevance, and an easy
response path. Account-specific claims must still come only from that account's
verified evidence.

`style-guide-rubric.md` records the cross-case human judgment standard used to
interpret those comparisons. It likewise must never be treated as prospect
evidence or pasted into production copy.
