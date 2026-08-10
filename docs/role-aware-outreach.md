# Role-aware outreach without psychographic guesswork

Spruce Leaf adapts outreach to the recipient's professional vantage. It does
not infer a private personality from LinkedIn, a biography, or a job title.

The distinction matters. A title can cautiously support an estimate of what a
person may know, the abstraction level at which they work, the social cost of
answering, and the size of request they can reasonably accept. It cannot prove
their motives, temperament, anxieties, politics, or buying authority.

## Evidence behind the design

- Huang et al. found that question asking, particularly responsive follow-up
  questions, increased interpersonal liking. In cold outreach this supports
  asking one informed question and using the answer to shape the next message,
  not sending a seven-question survey.
  <https://pubmed.ncbi.nlm.nih.gov/28447835/>
- Brooks, Gino, and Schweitzer found that seeking advice can increase perceived
  competence when the recipient has relevant expertise and the task is
  difficult. This supports treating an operator as the authority on their own
  workflow rather than flattering their résumé.
  <https://www.hbs.edu/ris/Publication%20Files/Advice%20Seeking_59ad2c42-54d6-4b32-8517-a99eeae0a45c.pdf>
- Magee, Milliken, and Lurie found an association between position power and
  more abstract language in a consequential organizational setting. This is a
  directional reason to ask operators about concrete incidents and senior
  leaders about materiality and ownership; it is not a license to stereotype
  an individual.
  <https://journals.sagepub.com/doi/10.1177/0146167209360418>
- Research on audience design shows that speakers adapt messages to what an
  addressee is expected to know. Spruce Leaf therefore treats likely knowledge
  scope as an explicit planning input.
  <https://pubmed.ncbi.nlm.nih.gov/31446659/>
- A digital-message experiment found a positive effect from providing choice,
  especially for people with a high need for autonomy. Cold outreach retains a
  voluntary, lower-cost reply path and does not force a meeting.
  <https://doi.org/10.2196/14074>
- Research on request formulation shows that communicators improve requests by
  anticipating the addressee's main obstacle to compliance. The response
  contract therefore names reply cost and face/reactance risk before copy is
  written.
  <https://doi.org/10.1016/0749-596X(85)90046-4>

These studies do not provide cold-email reply-rate estimates by job title.
They support mechanisms and guardrails. Actual role × message performance must
be learned from attributed campaign outcomes with randomized, single-variable
tests.

## Production role contracts

For each recipient, `src/response_design.rs` derives one compact contract from
the mapped vantage and verified title. Runtime copy receives only the question
shape, next step, and face/reactance guard; broader theory stays in this design
document because ablation showed that repeating it in the prompt degraded copy:

| Role | What the message should ask for |
| --- | --- |
| Operator | One actual moment, step, exception, or example |
| Frontline leader | A recurring team pattern and where intervention occurs |
| Process owner | How one decision works and whether the mechanism is material |
| Operational executive | Cross-operation consequence, ownership, or a route |
| Economic leader | Attribution, materiality, or the operating owner |
| Enterprise executive | Strategic relevance or one internal direction |
| Technical evaluator | A data/system boundary after business relevance exists |
| Commercial router | Whether their team becomes involved or who owns it |
| Router | One name or role for the bounded decision |

The contract enters all three independent stages:

1. Planning chooses the response, scene, evidence, and commitment.
2. Writing changes the question and ask for that role.
3. Review rejects mismatched detail, authority assumptions, reply cost, and
   face-threatening language.

No contract label or inferred motive may appear in buyer-facing copy.
