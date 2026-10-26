# Night Loop: Sanitized Reliability Results

**Date:** October 25, 2026

**Status:** Sanitized summary; raw run records withheld

**Evidence type:** Local verification across isolated scheduled and local experiments

## Purpose

Night Loop examined failure cases that are easy to obscure in a successful demo: STOP races, retries after uncertain outcomes, stale receipts, authority confusion, revision conflicts, and the difference between reproducible behavior and identical raw traces.

Each run was evaluated as an isolated invocation with explicit inputs and artifacts. The recurrence of scheduled invocations is evidence that the scheduler ran them—not that a model process stayed awake between runs.

## Run summary

| Run | Public-safe result | Interpretation |
| --- | --- | --- |
| 1 | An eight-test queue suite repeated five times: 40 test executions passed. Concurrency cases processed 120 tasks without duplicate database completions. Instruction order and execution order differed. A crash/retry case could yield two external effects but one recorded completion. | Ledger consistency does not prove exactly-once external effects. Behavioral invariants may hold while schedules and traces vary. |
| 2 | Nine test groups across four repetitions reproduced a STOP race: an already-dispatched action could finish after STOP. Destination idempotency reduced duplicates, but later loss of the key permitted duplication. | STOP is prospective, and idempotency is bounded by identity retention. |
| 3 | A deterministic model with 43 states and 75 transitions passed eight checks. Its conservative protocol made zero false prevention claims within the model. | Conservative state reporting can avoid claiming prevention without sufficient evidence; the result is limited to the modeled state space. |
| 4 | Across 511 simulated receipt-identity variants, a weak verifier made 255 false prevention claims; a complete-identity verifier made zero. Twenty assertions passed across five repetitions. | A receipt must bind the full operation identity. Even a strong verifier cannot force a destination to honor cancellation. |
| 5 | Nine local tests repeated twice: 18 executions passed. Revision and conflict integrity did not authenticate authority, and a rewritten hash chain could appear internally valid without an independent checkpoint. | Integrity, freshness, and authorization are distinct properties. |
| 6 | Ten assertions across two repetitions supported a safer commit pattern: the consumer obtains its own fresh revision token immediately before commit. | Freshness should be established by the committing consumer, not inherited from a possibly stale handoff. |
| 7 | Six experiment archives passed checksum/integrity checks and re-execution. Runs 2–6 reproduced exactly. Run 1 retained its tested behavioral properties, while four additional repetitions produced four different raw traces. | Specify behavioral invariants separately from byte-for-byte or trace equality. |

## Cross-run findings

- **STOP is not retroactive.** It can prevent future dispatch and stop cooperative work at a checkpoint, but it cannot undo an action already accepted elsewhere.
- **Retry needs stable identity.** At-least-once delivery plus receiver deduplication is more honest than an unsupported exactly-once claim.
- **Receipts need complete binding.** Operation, revision or generation, destination, authority scope, and action hash all matter.
- **Integrity is not authority.** A structurally valid revision or hash chain does not prove that its author was authorized.
- **Reproducibility has layers.** Reproducing the same safety property is different from reproducing an identical execution trace.

## Observed, inferred, and not established

| Classification | Statement |
| --- | --- |
| Observed | Isolated scheduled/local invocations produced artifacts that could be checked and, for the archived experiments, re-executed. |
| Inferred | Durable inputs and explicit artifacts can support later independent verification. |
| Not established | A continuously awake process, autonomous idle wake-up of a prior conversation, transferred subjective state, or inter-run continuity. |

## Provenance and privacy

This page preserves aggregate counts, tested properties, and limitations from the run records. Raw archives, scheduler metadata, transcripts, identifiers, local paths, receipts, and screenshots are withheld because they contain private or operational context. Direct Home–Work delivery was not established by these Night Loop runs.
