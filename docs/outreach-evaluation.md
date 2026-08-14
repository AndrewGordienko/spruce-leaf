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
4. Promotion requires the current brand sample (GnK 30 with 6 sealed holdouts, Wapahki 10 with 2, OutageHub 40 with 10), double-blind order checking, at least 90% selection and absolute-sendability accuracy for every brand, at least 90% accuracy on each sealed holdout, and zero unsupported claims in a human-labelled sendable draft.
5. Model scores authorize no sends. Use human approval as the pre-send gate, then judge the motion by positive-reply rate and qualified conversations on randomized account-level arms.

For a real inventory audit, run `cargo run -- --brand <brand> acceptance-report
--output artifacts/acceptance/<date>`. The command is read-only: it creates the
brand's governed company sample, qualification decisions, contact-evidence
records, candidate/selector inventory, blank Andrew review sheet, cross-account
similarity report, and grouped failure report. It never researches, generates,
labels, approves, schedules, or sends. Empty candidate output is a valid and
important result when no account passes the pre-writing gates.

The writer, editor, and verifier have independent controls:

- `SPRUCE_OPENAI_WRITER_MODEL` / `SPRUCE_OPENAI_WRITER_REASONING_EFFORT`
- `SPRUCE_OPENAI_EDITOR_MODEL` / `SPRUCE_OPENAI_EDITOR_REASONING_EFFORT`
- `SPRUCE_OPENAI_VERIFIER_MODEL` / `SPRUCE_OPENAI_VERIFIER_REASONING_EFFORT`

This lets Sol/xhigh compete as the writer without allowing the same model to author, repair, and approve its own style.
