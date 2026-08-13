# Outreach evaluation

Production promotion is evidence-based, not a model-name decision.

The bundled corpus combines anonymized rejected-draft contrasts with Andrew's human-written
OutageHub, GnK, and Wapahki/Morrow reference messages. Those references are validation anchors,
not templates or prospect facts. Keep expanding it with real before/after decisions and maintain a
holdout that prompt authors cannot see.

1. Add anonymized human before/after pairs to `evals/outreach-gold.jsonl`. Keep
   `verified_facts` (prospect) separate from `verified_seller_facts` (Andrew's
   truthful capabilities), and fix the human preference before running a model.
2. Change one variable at a time: writer model, reasoning effort, prompt version, or research bundle. Do not change the list and copy system together.
3. Run `cargo run -- eval-outreach --double-blind`. The verifier judges both candidate orders; order-inconsistent cases fail.
4. Promotion requires at least 30 cases, double-blind order checking, at least 90% pairwise agreement, at least 90% absolute sendability accuracy across 20 or more human labels, and zero unsupported claims in a human-labelled sendable draft.
5. Model scores authorize no sends. Use human approval as the pre-send gate, then judge the motion by positive-reply rate and qualified conversations on randomized account-level arms.

The writer, editor, and verifier have independent controls:

- `SPRUCE_OPENAI_WRITER_MODEL` / `SPRUCE_OPENAI_WRITER_REASONING_EFFORT`
- `SPRUCE_OPENAI_EDITOR_MODEL` / `SPRUCE_OPENAI_EDITOR_REASONING_EFFORT`
- `SPRUCE_OPENAI_VERIFIER_MODEL` / `SPRUCE_OPENAI_VERIFIER_REASONING_EFFORT`

This lets Sol/xhigh compete as the writer without allowing the same model to author, repair, and approve its own style.
