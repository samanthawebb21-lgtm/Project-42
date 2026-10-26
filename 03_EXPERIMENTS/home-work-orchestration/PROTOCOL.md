# Public Orchestration Protocol

## Envelope

Each incoming instruction is normalized before it can affect task state:

```json
{
  "protocol_version": 1,
  "message_id": "delivery-001",
  "idempotency_key": "semantic-operation-001",
  "task_id": "task-001",
  "parent_task_id": null,
  "type": "task.create",
  "task_revision": 1,
  "expected_revision": null,
  "source_surface": "home",
  "authored_at": "2030-01-01T00:00:00Z",
  "priority": 50,
  "authority_class": "local_reversible",
  "correlation_id": null,
  "payload": {}
}
```

The receiver computes a canonical content hash. If the sender supplies a hash, it must match.

## Ordering and duplicate suppression

1. The local database assigns an ingest sequence in one immediate transaction.
2. Delivery identity (`message_id`) is unique.
3. Semantic identity (`source_surface`, `idempotency_key`) is unique.
4. A duplicate is recorded and ignored; it does not execute again.
5. Queued work is ordered by priority, then by local creation sequence.

Remote timestamps are provenance, not execution order.

## Revisions and stale receipts

A correction or priority change must name the revision it expects and advance by one revision. A mismatch is quarantined. A worker must complete against the revision it observed; completion against a superseded revision is rejected.

A receipt is valid only when it binds the complete operation identity, including the operation ID, task revision or generation, destination, authority scope, and action hash. Matching only payload text is insufficient: an authentic receipt for an older, visually identical action can be stale.

## Task states

```text
QUEUED -> CLAIMED -> RUNNING -> SUCCEEDED
                      |  |  |
                      |  |  +-> FAILED
                      |  +----> BLOCKED -> QUEUED
                      +-------> CANCEL_REQUESTED -> CANCELLED

CLAIMED or RUNNING --expired lease--> ORPHANED -> CLAIMED
```

`PAUSED` and `WAITING_APPROVAL` are reserved states. Recovery never guesses the outcome of a consequential external effect.

## Leases and fencing

Every claim includes a worker identity, lease epoch, last heartbeat, and expiry. Recovery advances the epoch. Updates from an older epoch fail even if the previous worker later returns.

## STOP and cancellation

Cancellation is revision-tolerant and dominates correction and priority changes.

- Queued work can become cancelled before dispatch.
- Running work observes cancellation at a safe checkpoint and explicitly acknowledges that it stopped.
- A cancellation that arrives before creation forms a tombstone, preventing a delayed create from resurrecting the task.
- Cancellation cannot retroactively prevent an external effect that was already dispatched.

Reports therefore distinguish four facts:

1. cancellation requested;
2. worker stopped;
3. external outcome unknown; and
4. prevention confirmed by operation-bound evidence.

## Side effects and authority

Before a consequential action:

1. assign a stable operation ID;
2. record the exact action, destination, authority class, and action hash;
3. obtain operation-specific approval when required;
4. record `INTENT_RECORDED` before dispatch; and
5. record `COMPLETED`, `FAILED`, or `OUTCOME_UNKNOWN` afterward.

`OUTCOME_UNKNOWN` blocks automatic retry until the real-world result is reconciled. This avoids converting uncertainty into duplicate side effects.

## Retry model

Delivery is at least once. The sender retries byte-identical content under a stable identity until acknowledgement. The receiver deduplicates by identity and records the duplicate. Destination idempotency can reduce duplicate effects only within the destination's identity and retention scope.

An exactly-once database transition is not evidence of an exactly-once external effect.

## Immutable transport history

Corrections and cancellations are new events; they do not rewrite prior events. If a transport cursor becomes stale or history is compacted, the consumer performs a full resynchronization and deduplicates events rather than inferring missing content from sequence gaps.
