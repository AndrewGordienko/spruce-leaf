# Outreach prompt ablation

Spruce Leaf treats prompt length as an engineering cost, not a proxy for copy
quality. The ablation command generates the same first email under prompt
variants that remove or resize one layer at a time, then compares each variant
with the production prompt through the existing blind human-style evaluator.

```sh
cargo run -- eval-outreach-ablation --cases 3 --repeats 1
cargo run -- eval-outreach-ablation --cases 3 --repeats 2 --show-drafts
```

The default cases are one human-anchored validation example per brand. Every
arm holds these constant:

- account, recipient, title, and requested cold outcome;
- verified account facts and verified seller facts;
- the hypothesis, which remains explicitly unverified;
- model, structured output schema, and blind evaluation rubric.

The arms remove the compact psychology layer, remove the writer persona, remove
brand doctrine, shrink the writer persona, or expand psychology. Each comparison
is judged in both candidate orders. The report also runs absolute checks for the
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

These are directional model-quality results from a small validation set, not
reply-rate evidence. A live campaign should still test one copy variable at a
time, split infrastructure and timing evenly, and use positive reply rate after
the full sequence as the primary outcome. Small live samples are descriptive;
they are not statistically persuasive.
