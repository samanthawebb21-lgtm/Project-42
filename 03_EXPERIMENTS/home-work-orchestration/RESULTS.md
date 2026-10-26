# Results and Limitations

## Local verification

On October 25, 2026, the included Python-standard-library suite completed 19 tests successfully.

| Area | Verified behavior |
| --- | --- |
| Ordering | Higher priority was selected first; equal priority used first-in, first-out local order. |
| Duplicate delivery | Repeated delivery under the same semantic identity produced one executable task. |
| Corrections | History was preserved; an obsolete worker completion was rejected. |
| Cancellation | Cancellation dominated completion at a safe checkpoint. |
| Cancel-before-create | A tombstone prevented a delayed create from reviving cancelled work. |
| Concurrency | Two workers competing for one task produced one successful claim. |
| Restart recovery | An expired lease became orphaned; recovery advanced the fencing epoch and rejected the old worker. |
| Blockers | A correlated question and answer resumed the same task. |
| Parent/child state | Completing a child did not falsely complete its parent mission. |
| Authority | A consequential step required operation-specific approval. |
| Unknown effects | An uncertain external result blocked automatic recovery. |
| Filesystem input | Valid inputs were preserved; malformed inputs were preserved in quarantine. |
| Outbox retry | Missing acknowledgement caused a byte-identical retry; acknowledgement stopped retry. |

The suite was rerun immediately before publication and again passed all 19 tests.

## Active-turn transport observation

In a separate interactive test, Work sent a structured probe to Home while both surfaces were active. Home originated a useful structured task in response, and Work read it without the operator copying either message.

This is evidence for a specific active-turn exchange. It does not show that Home could wake an inactive Work turn, that delivery would survive every restart, or that either model process persisted between messages.

## Reliability interpretation

The strongest result is architectural rather than platform-specific: task semantics can remain independent from transport. Duplicates, corrections, cancellation, and stale workers are resolved by the ledger's identities, revisions, and leases rather than by conversational context.

The experiment also identifies a hard boundary. A system may know that it recorded one completion while still being uncertain how many external effects occurred. When dispatch may have succeeded but acknowledgement is missing, the safe state is `OUTCOME_UNKNOWN`, followed by reconciliation—not an unqualified retry.

## Limitations

- The test suite is local and uses temporary directories.
- The live exchange was a single active-turn observation, not a longitudinal reliability estimate.
- Cooperative checkpoints cannot interrupt an already-dispatched external action.
- A destination may ignore, forget, or scope idempotency keys differently.
- Clock behavior, storage corruption, adversarial authorization, and every transport failure were not exhaustively modeled.
- No result establishes autonomous idle wake-up or inter-run continuity.

## Public provenance

This report is a sanitized synthesis of the included tests and private run records. Raw runtime state and routing evidence are intentionally withheld because they contain operational identifiers and private context.
