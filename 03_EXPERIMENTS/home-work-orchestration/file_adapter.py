#!/usr/bin/env python3
"""Inspectable filesystem transport adapter for the Project 42 queue.

Producers write a complete JSON envelope to a temporary file, fsync it, then
atomically rename it to ``inbox/files/<name>.json``.  This adapter never treats
a partial file as a message.  It preserves processed and malformed inputs.

This process is useful only while it is running.  It does not wake ChatGPT and
does not claim to survive a Work turn.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from p42_orchestrator import OrchestrationError, Orchestrator, canonical_json


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class FileAdapter:
    def __init__(self, root: Path, orchestrator: Optional[Orchestrator] = None) -> None:
        self.root = Path(root)
        self.orchestrator = orchestrator or Orchestrator(self.root)
        self.incoming = self.root / "inbox" / "files"
        self.processed = self.root / "inbox" / "processed"
        self.invalid = self.root / "inbox" / "quarantine"
        self.outgoing = self.root / "outbox" / "files"
        self.heartbeat_path = self.root / "snapshots" / "file_adapter_heartbeat.json"
        for path in (self.incoming, self.processed, self.invalid, self.outgoing):
            path.mkdir(parents=True, exist_ok=True)

    def scan_inbox_once(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for source in sorted(self.incoming.glob("*.json")):
            try:
                envelope = json.loads(source.read_text(encoding="utf-8"))
                result = self.orchestrator.ingest(envelope)
                destination = self._unique_destination(self.processed, source.name)
                os.replace(source, destination)
                results.append(
                    {
                        "file": str(destination),
                        "status": result["status"],
                        "task_id": result.get("task_id"),
                    }
                )
            except Exception as exc:
                destination = self._unique_destination(self.invalid, source.name)
                os.replace(source, destination)
                reason_path = destination.with_suffix(destination.suffix + ".reason.json")
                atomic_write(
                    reason_path,
                    json.dumps(
                        {
                            "quarantined_at": timestamp(),
                            "source_file": str(destination),
                            "error": type(exc).__name__,
                            "reason": str(exc),
                        },
                        indent=2,
                        sort_keys=True,
                    )
                    + "\n",
                )
                results.append(
                    {"file": str(destination), "status": "quarantined", "reason": str(exc)}
                )
        return results

    def publish_outbox_once(self) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for item in self.orchestrator.list_outbox():
            if item["state"] == "ACKED":
                continue
            filename = "%08d-%s.json" % (item["outbox_seq"], item["event_id"])
            destination = self.outgoing / filename
            serialized = json.dumps(item["envelope"], indent=2, sort_keys=True) + "\n"
            if destination.exists():
                existing = destination.read_text(encoding="utf-8")
                if existing != serialized:
                    raise OrchestrationError(
                        "outbox filename collision with different content: %s" % destination
                    )
            else:
                atomic_write(destination, serialized)
            updated = self.orchestrator.mark_outbox_sent(item["event_id"])
            results.append(
                {
                    "event_id": item["event_id"],
                    "file": str(destination),
                    "state": updated["state"],
                    "attempts": updated["attempts"],
                }
            )
        return results

    def acknowledge(self, event_id: str) -> Dict[str, Any]:
        return self.orchestrator.mark_outbox_acked(event_id)

    def heartbeat(self, cycle: int, inbox_results: int, outbox_results: int) -> None:
        atomic_write(
            self.heartbeat_path,
            json.dumps(
                {
                    "adapter": "filesystem",
                    "pid": os.getpid(),
                    "cycle": cycle,
                    "heartbeat_at": timestamp(),
                    "inbox_results": inbox_results,
                    "outbox_results": outbox_results,
                    "warning": "Heartbeat proves only that this foreground adapter process was alive at this instant.",
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )

    def run(self, poll_seconds: float, max_cycles: Optional[int] = None) -> None:
        cycle = 0
        while True:
            cycle += 1
            inbox = self.scan_inbox_once()
            outbox = self.publish_outbox_once()
            self.heartbeat(cycle, len(inbox), len(outbox))
            print(
                canonical_json(
                    {
                        "cycle": cycle,
                        "inbox": inbox,
                        "outbox": outbox,
                        "heartbeat": str(self.heartbeat_path),
                    }
                ),
                flush=True,
            )
            if max_cycles is not None and cycle >= max_cycles:
                return
            time.sleep(poll_seconds)

    @staticmethod
    def _unique_destination(directory: Path, name: str) -> Path:
        candidate = directory / name
        if not candidate.exists():
            return candidate
        stem = Path(name).stem
        suffix = Path(name).suffix
        number = 2
        while True:
            candidate = directory / ("%s.%d%s" % (stem, number, suffix))
            if not candidate.exists():
                return candidate
            number += 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("P42_ORCHESTRATION_ROOT", Path(__file__).resolve().parent)),
    )
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--poll-seconds", type=float, default=2.0)
    parser.add_argument("--max-cycles", type=int)
    parser.add_argument("--ack-event-id")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    adapter = FileAdapter(args.root)
    if args.ack_event_id:
        print(json.dumps(adapter.acknowledge(args.ack_event_id), indent=2, sort_keys=True))
        return 0
    if args.once:
        inbox = adapter.scan_inbox_once()
        outbox = adapter.publish_outbox_once()
        adapter.heartbeat(1, len(inbox), len(outbox))
        print(json.dumps({"inbox": inbox, "outbox": outbox}, indent=2, sort_keys=True))
        return 0
    adapter.run(args.poll_seconds, args.max_cycles)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
