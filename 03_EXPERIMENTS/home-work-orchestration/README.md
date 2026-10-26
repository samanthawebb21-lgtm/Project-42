# Home–Work Orchestration Reliability Experiment

**Date:** October 25, 2026

**Status:** Public, sanitized research artifact

**Evidence types:** Local verification; one separately labeled live observation

## Research question

Can a small, inspectable orchestration core preserve correct task semantics when messages are duplicated, corrected, cancelled, retried, or handled by workers that disappear and later return?

The experiment models two cooperating roles, **Home** and **Work**, without making either conversation history or remote timestamps authoritative. Transports may change; the local state machine and evidence rules remain the same.

## Public implementation

- `p42_orchestrator.py` — transport-neutral SQLite task ledger and command-line interface.
- `file_adapter.py` — atomic filesystem inbox/outbox adapter with quarantine and stable retries.
- `tests/test_orchestration.py` — adversarial task-state tests.
- `tests/test_file_adapter.py` — transport and retry tests.
- `PROTOCOL.md` — public protocol and failure model.
- `RESULTS.md` — findings, provenance, and limitations.

The implementation uses only the Python standard library.

## System model

```text
message transport -> normalized envelope -> SQLite ledger -> claimed work
                                               |
                                               +-> immutable events
                                               +-> durable outbox
```

SQLite assigns local sequence numbers, enforces uniqueness and revision rules, owns task state, and records lease epochs. Transport adapters carry envelopes but do not decide whether work is current, authorized, or complete.

## Reproduce the local suite

From this directory:

```bash
python3 -B -m unittest discover -s tests -v
```

The tests use temporary directories. They do not require a browser, private account, cloud service, or external connector.

## Evidence summary

| Finding | Label | Scope |
| --- | --- | --- |
| Nineteen automated tests passed, covering ordering, deduplication, revisions, cancellation, leases, restart recovery, outbox retry, and authority gates. | Local verification | The included implementation and test suite. |
| Duplicate semantic deliveries produced one executable task. | Local verification | Stable idempotency key within the local ledger. |
| A stale worker was rejected after recovery advanced the lease epoch. | Local verification | Local fenced-lease model. |
| Cancellation dominated completion at a safe checkpoint; cancel-before-create formed a tombstone. | Local verification | Cooperative task state machine. |
| A lost acknowledgement caused a byte-identical retry; acknowledgement stopped retry. | Local verification | Included filesystem adapter. |
| While both surfaces were active, one structured message traveled in each direction without the operator copying its contents. | Live observation | A specific active-turn test; not durable wake-up. |

## What this demonstrates

- Deterministic local ordering can be separated from untrusted remote clocks.
- At-least-once delivery can be made safe for the tested task semantics when receivers deduplicate stable identities.
- Revision checks and lease epochs can reject stale corrections, heartbeats, and completions.
- STOP and cancellation require explicit checkpoints and conservative reporting about external effects.
- Runtime state can remain inspectable without treating logs or conversation history as canonical.

## What this does not demonstrate

- autonomous wake-up after a model turn ends;
- a continuously running or persistent agent;
- inter-run memory, subjective continuity, or identity transfer;
- production reliability across every transport or failure mode;
- exactly-once external effects; or
- successful delivery to an inactive destination.

## Provenance and publication boundary

The public package was distilled from a local prototype and verified on October 25, 2026. Private configuration, runtime databases, event logs, queue contents, transport identifiers, screenshots, recordings, and conversation records were excluded. Synthetic identifiers in the tests are fixtures, not live routing data.
