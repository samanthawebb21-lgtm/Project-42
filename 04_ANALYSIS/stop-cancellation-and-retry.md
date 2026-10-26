# STOP, Cancellation, Retry, and Stale Receipts

## Executive finding

STOP is a protocol event, not a time machine.

A reliable system must distinguish stopping future work from proving that no external effect occurred. Once an action has crossed a system boundary, cancellation may race with completion, acknowledgements may be lost, and a legitimate receipt may describe an older operation that only looks identical to the current one.

## Four states that should not be collapsed

1. **Cancellation requested** — the request was accepted by the local ledger.
2. **Worker stopped** — the current worker observed cancellation at a safe checkpoint and acknowledged it.
3. **External outcome unknown** — dispatch may have occurred, but the result is not operation-bound and verified.
4. **Prevention confirmed** — evidence tied to the exact operation establishes that the effect did not occur.

Reporting “cancelled” when only the first state is known overclaims what the system can prove.

## STOP semantics

- STOP dominates queued corrections and priority changes.
- A cancel-before-create tombstone prevents a delayed create from reviving stopped work.
- A running worker checks for STOP before and after meaningful steps.
- Lease epochs fence a stale worker after recovery assigns the task to a replacement.
- An already-dispatched external action may still complete after STOP.

These semantics are cooperative. They do not imply that an external provider supports cancellation or that an in-flight effect can be recalled.

## Duplicate and retry semantics

The tested design uses at-least-once delivery:

1. assign stable delivery and semantic identities;
2. persist the exact outgoing bytes;
3. retry the same bytes under the same identity until acknowledgement; and
4. make the receiver deduplicate that identity.

This prevents duplicate task execution in the tested ledger. It does not prove that an external destination performed exactly one effect. Provider-side idempotency helps only while the provider retains and interprets the key in the expected scope.

If a crash occurs after dispatch but before the result is recorded, mark the operation `OUTCOME_UNKNOWN`. Reconcile the real-world result before retrying. A blind retry converts uncertainty into duplicate-effect risk.

## Why stale receipts are dangerous

A receipt can be authentic and still be irrelevant to the current action. Payload text alone is not an operation identity.

A prevention or completion receipt should bind at least:

- operation ID;
- task revision or generation;
- destination and action type;
- authority scope;
- hash of the complete action details; and
- provider result or acknowledgement identity when available.

The Night Loop receipt experiment demonstrated the distinction sharply: across 511 simulated identity variants, a weak verifier produced 255 false prevention claims, while a verifier bound to complete operation identity produced zero within the model.

## Freshness and authority

Revision checks answer whether a write is current relative to a known state. They do not answer whether the writer is authorized. Hash-chain consistency proves internal linkage, not independent authorship or absence of a full-history rewrite.

For a consequential commit, the consumer should obtain its own fresh revision token immediately before committing and validate authorization separately. Independent checkpoints improve tamper evidence.

## Conservative reporting rule

Use the strongest statement supported by operation-bound evidence:

| Evidence | Safe statement |
| --- | --- |
| STOP recorded locally | “Cancellation was requested.” |
| Current worker acknowledged a checkpoint | “The worker stopped before its next recorded step.” |
| Dispatch status cannot be reconciled | “The external outcome is unknown; no retry was issued.” |
| Exact provider evidence shows no effect | “Prevention was confirmed for this operation.” |
| Exact provider evidence shows completion | “Completion was confirmed for this operation.” |

Do not translate a successful local state transition into a stronger claim about an external system.

## Evidence basis and limits

These conclusions synthesize the [Home–Work orchestration tests](../03_EXPERIMENTS/home-work-orchestration/RESULTS.md) and the [Night Loop experiments](../03_EXPERIMENTS/night-loop-2026-10-25/README.md). They are operational reliability findings from specific local implementations and models, not platform-wide guarantees. They do not establish autonomous idle wake-up or inter-run continuity.
