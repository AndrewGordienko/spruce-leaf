# Outreach prompt ablation

Spruce Leaf treats prompt length as an engineering cost, not a proxy for copy
quality. The ablation command generates the same first email under prompt
variants that remove or resize one layer at a time, then compares each variant
with the production prompt through the existing blind human-style evaluator.

```sh
cargo run -- eval-outreach-ablation --cases 3 --repeats 1
cargo run -- eval-outreach-ablation --cases 3 --repeats 2 --show-drafts
cargo run -- eval-outreach-ablation --cases 3 --only no-role-contract --show-drafts
```

The default cases are one human-anchored validation example per brand. Every
arm holds these constant:

- account, recipient, title, and requested cold outcome;
- verified account facts and verified seller facts;
- the hypothesis, which remains explicitly unverified;
- model, structured output schema, and blind evaluation rubric.

The arms remove the per-recipient role contract, remove the compact psychology
layer, remove the writer persona, remove brand doctrine, shrink the writer
persona, or expand psychology. Use `--only` to retest one variant against the
full prompt without paying to regenerate unrelated arms. Each comparison is
judged in both candidate orders. The report also runs absolute checks for the
subject word band, generic subject labels, greeting, signature, and body length.
A relative winner with an absolute failure is not sendable.

## Current decision

The 2026-08-09 diagnostic first found that expanding both writer and psychology
together beat the compact prompt in three of three cases, but that combined arm
could not identify the cause. The follow-up isolated each layer:

- Expanding only the writer excerpt from 120 to 360 words won two of three
  cross-brand comparisons and passed absolute QA in all three, for roughly 250
  extra prompt words. This became the production setting.
- Expanding psychology from 130 to 300 words was mixed, so psychology remains
  compact.
- Removing psychology reduced mechanical reliability and produced unstable
  blind verdicts, so the layer remains.
- Removing brand doctrine sometimes made a draft safer but also removed
  brand-specific relevance. Contradictory brand instructions should be repaired
  individually rather than deleting the layer.
- Deterministic QA caught invalid two-word and topical-label subjects that a
  semantic judge occasionally preferred. Mechanical and semantic review remain
  separate because neither subsumes the other.

The role-contract follow-up also exposed prompt bloat. An initial 142-word
contract changed the copy but lost four of six order-consistent comparisons to
the no-contract arm across two one-repeat runs. It was therefore reduced to a
66-word role instruction containing only the question shape, next step, and
face/reactance guard. In the immediate three-brand retest, all variants passed
absolute QA; the compact contract won one comparison and the other two were
order-inconsistent. The compact layer remains because it materially improved
the finance-role case and no longer displaced the core message with role theory,
but it should be retested against live positive replies rather than expanded.

These are directional model-quality results from a small validation set, not
reply-rate evidence. A live campaign should still test one copy variable at a
time, split infrastructure and timing evenly, and use positive reply rate after
the full sequence as the primary outcome. Small live samples are descriptive;
they are not statistically persuasive.

## Production scaling result

A separate live scaling check found that batching three seven-touch recipients
into one writer request exhausted the 12,288-token output ceiling, and Sol at
`xhigh` exhausted that ceiling on three of four writer attempts. The production
path now isolates one recipient per writer call and defaults the writer to
`high`. In the same rejected-recipient case, the isolated `high` request
completed at 7,075 output tokens and passed review. Higher reasoning and larger
batches were consuming the answer budget without producing more usable copy.
