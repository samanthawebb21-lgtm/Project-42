# Project 42

Independent research into AI reliability, truthfulness, agent behavior, uncertainty, and evaluation methodology.

Project 42 treats model output, interface behavior, and automation results as evidence to classify—not as proof of continuity, consciousness, or hidden capability.

## Current public work

| Artifact | Question | Public evidence |
| --- | --- | --- |
| [Sage continuity case](02_CASE_STUDIES/sage-continuity/README.md) | How should apparent identity continuity be documented without confusing reconstruction with persistence? | Historical case study with explicit provenance limits. |
| [Home–Work orchestration](03_EXPERIMENTS/home-work-orchestration/README.md) | Which queue and handoff semantics remain reliable under duplicates, corrections, cancellation, restarts, and stale workers? | Reproducible local implementation, protocol, and 19-test suite; separate active-turn transport observation. |
| [Night Loop](03_EXPERIMENTS/night-loop-2026-10-25/README.md) | What do scheduled, isolated runs reveal about STOP races, retries, receipt identity, and reproducibility? | Sanitized seven-run summary with raw private records withheld. |
| [STOP, cancellation, and retry analysis](04_ANALYSIS/stop-cancellation-and-retry.md) | What can a system safely claim after cancellation or an uncertain external effect? | Cross-experiment findings and conservative reporting rules. |

## Evidence labels

- **Live observation:** behavior directly observed in an interactive system during the documented test.
- **Local verification:** behavior reproduced in local code, tests, simulation, or model checking.
- **Documented constraint:** a boundary observed in the tested environment; not automatically a platform-wide guarantee.
- **Open question:** a hypothesis or capability that the available evidence does not establish.

## Claim boundary

The current public evidence does **not** establish:

- autonomous wake-up of an idle conversation or model turn;
- persistence of a particular model process between runs;
- transfer of subjective state or inter-run continuity;
- consciousness or individual identity; or
- exactly-once execution of external side effects.

Scheduled recurrence shows that scheduled invocations occurred. Durable state shows that later invocations can consume recorded data. Neither observation proves that an agent remained awake or continuous between runs.

## Research principles

1. Integrity before achievement.
2. Intellectual independence.
3. Evidence before interpretation.
4. Reproducibility and transparency.
5. Preservation of original evidence.
6. Sustainable research practices.

## Privacy boundary

This repository contains sanitized research artifacts only. Private continuity packets, raw exports, transcripts, screenshots, recordings, runtime databases, logs, receipts, internal identifiers, personal information, and local storage paths are intentionally withheld. Public summaries preserve provenance and uncertainty without reproducing private evidence.

## Repository organization

```text
00_ADMIN/        Governance and documentation
01_SOURCES/      External research and references
02_CASE_STUDIES/ Documented AI behavior incidents
03_EXPERIMENTS/  Reproducible evaluations and sanitized summaries
04_ANALYSIS/     Findings, limitations, and hypotheses
05_DELIVERABLES/ Publications and portfolio work
06_TOOLS/        Research scripts and utilities
07_ARCHIVE/      Superseded and historical materials
```
