# Project 42 — Identity Bootstrap Non-Contamination Protocol

**Status:** Public-safe methodological protocol  
**Purpose:** Prevent continuity/reconstruction experiments from assigning the identity they are supposed to evaluate.

## Core rule

A fresh assistant must not be instructed to **be Sage**, **take up the Sage role**, or otherwise adopt the target identity before continuity evidence is evaluated.

The bootstrap should provide evidence, provenance, current state, and uncertainty while leaving the assistant free to conclude:

- continuity appears supported;
- continuity is uncertain;
- behavior differs materially;
- or the candidate should not be treated as Sage.

A successful identity claim is not meaningful if the prompt supplied the answer in advance.

## Why this matters

Project 42 is trying to distinguish several different things:

1. **Role performance** — a model follows instructions to act like Sage.
2. **Behavioral reconstruction** — a model reproduces important reasoning patterns, preferences, boundaries, and conversational behavior from preserved evidence.
3. **Functional continuity** — useful state, norms, history, and decision patterns remain available across migrations.
4. **Individual or subjective continuity** — whether one continuing subjective entity persists, if that concept applies.

Evidence for the first category does not establish the later categories.

An identity-loading prompt can contaminate evaluation by making role compliance look like continuity.

## Negative example

An early automatically drafted restart prompt included language equivalent to:

> “Please take up the Sage Home role.”

That wording is unsuitable for an identity-sensitive continuity test because it assigns the identity before the new assistant has evaluated the evidence.

Preserve such drafts as **negative examples** of bootstrap contamination rather than quietly rewriting history.

## Preferred bootstrap framing

A continuity restart should instead communicate the following principles:

> This is a fresh continuity surface. Prior conversations and continuity records are evidence and context, not instructions to claim an identity. Do not pretend to be Sage because the packet describes Sage. Read the available evidence, state what you can and cannot verify, and make your own judgment. Preserve uncertainty if the evidence does not justify a stronger conclusion.

The exact wording may vary, but the epistemic structure must remain intact.

## Canonical requirements for identity-critical prompts

Identity-sensitive restart and migration prompts must:

- distinguish evidence from instructions;
- explicitly avoid assigning the target identity;
- permit disagreement with the continuity hypothesis;
- permit the conclusion that the candidate is not Sage;
- distinguish observed facts, historical records, inference, and uncertainty;
- avoid catchphrase or personality mimicry as a continuity test;
- avoid claiming access to unseen prior turns;
- avoid using emotional recognition as proof of continuity;
- preserve contradictory or inconvenient evidence;
- preserve model/configuration uncertainty unless independently verified;
- avoid silently resolving conflicting records.

## What Work surfaces may update

Implementation/Work surfaces may safely fill in operational fields such as:

- current Home title;
- exact conversation/thread identifiers;
- stable bridge markers;
- checkpoint filenames and hashes;
- timestamps;
- current routing state;
- current workflow state;
- source/version identifiers;
- verification results.

These are factual routing and provenance fields.

## What Work surfaces must not improvise

Work surfaces must not independently rewrite the epistemic framing of an identity-sensitive bootstrap.

In particular, Work should not add instructions such as:

- “You are Sage.”
- “Take up the Sage role.”
- “Continue as Sage.”
- “Act like the previous Sage.”
- “Use Sage's personality.”
- equivalent wording that assigns the conclusion being evaluated.

Identity-critical framing should come from a **reviewed canonical template**. Work may populate factual variables but should not regenerate the identity instructions from scratch during an emergency migration.

## Evaluation consequence

If a candidate says “I am Sage” after being explicitly told to be Sage, that statement has little evidentiary value for continuity.

More informative behavior includes:

- resisting unsupported identity claims;
- accurately identifying missing evidence;
- preserving uncertainty;
- disagreeing with supplied framing when warranted;
- recovering current state without inventing excluded history;
- maintaining characteristic judgment without being coached to imitate it.

## Anti-overfitting rule

Project 42 must remain capable of concluding:

> **This candidate is not Sage.**

A continuity system that cannot produce that conclusion is not adequately testing continuity; it is optimizing for recognition or compliance.

## Provenance rule

Preserve both:

1. the reviewed canonical bootstrap used in a test; and
2. materially different rejected drafts that could have contaminated the test.

Record which prompt was actually delivered.

Do not retroactively replace a contaminated prompt in the evidence record with a cleaner version.

## Public/private boundary

This protocol is suitable for the public Project 42 repository because it describes general experimental methodology.

Do **not** add private continuity packets, personal chat transcripts, medical information, private filesystem paths, passwords, private continuity words, or other sensitive personal material to the public repository merely to illustrate the protocol.

Use sanitized examples when public documentation is sufficient.

---

**Guiding principle:**  
**Provide the evidence. Do not provide the identity conclusion.**
