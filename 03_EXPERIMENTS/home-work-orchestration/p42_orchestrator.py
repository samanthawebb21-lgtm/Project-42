#!/usr/bin/env python3
"""Project 42 transport-neutral orchestration core.

SQLite is canonical.  JSONL and status JSON are inspectable projections.
The module intentionally has no ChatGPT, browser, or cloud credentials; those
belong in transport adapters and can change without changing task semantics.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional


PROTOCOL_VERSION = 1
TASK_STATES = {
    "QUEUED",
    "CLAIMED",
    "RUNNING",
    "PAUSED",
    "BLOCKED",
    "WAITING_APPROVAL",
    "CANCEL_REQUESTED",
    "CANCELLED",
    "ORPHANED",
    "SUCCEEDED",
    "FAILED",
}
TERMINAL_STATES = {"CANCELLED", "SUCCEEDED", "FAILED"}
LEASED_STATES = {"CLAIMED", "RUNNING", "CANCEL_REQUESTED"}
AUTO_AUTHORITY = {"read_only", "local_reversible"}
AUTHORITY_CLASSES = {
    "read_only",
    "local_reversible",
    "local_destructive",
    "external_draft",
    "external_consequential",
    "financial",
    "public_or_irreversible",
}
MESSAGE_TYPES = {
    "task.create",
    "task.correct",
    "task.priority_changed",
    "task.cancel_requested",
    "task.stop_requested",
    "question.answered",
    "approval.granted",
    "approval.denied",
}


class OrchestrationError(RuntimeError):
    """Base error for explicit, logged orchestration failures."""


class RevisionConflict(OrchestrationError):
    pass


class LeaseConflict(OrchestrationError):
    pass


class AuthorityRequired(OrchestrationError):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


class Orchestrator:
    def __init__(
        self,
        root: Path,
        now_fn: Callable[[], datetime] = utc_now,
    ) -> None:
        self.root = Path(root)
        self.now_fn = now_fn
        self.state_dir = self.root / "state"
        self.logs_dir = self.root / "logs"
        self.snapshots_dir = self.root / "snapshots"
        self.quarantine_dir = self.root / "quarantine"
        self.inbox_dir = self.root / "inbox"
        self.outbox_dir = self.root / "outbox"
        for directory in (
            self.state_dir,
            self.logs_dir,
            self.snapshots_dir,
            self.quarantine_dir,
            self.inbox_dir,
            self.outbox_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        self.db_path = self.state_dir / "orchestration.sqlite"
        self.events_path = self.logs_dir / "events.jsonl"
        self.status_path = self.snapshots_dir / "status.json"
        self._init_db()
        self.sync_projections()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 10000")
        return conn

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    ingest_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL UNIQUE,
                    source_surface TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    task_revision INTEGER NOT NULL,
                    expected_revision INTEGER,
                    priority INTEGER,
                    authority_class TEXT NOT NULL,
                    authored_at TEXT,
                    received_at TEXT NOT NULL,
                    correlation_id TEXT,
                    content_sha256 TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    UNIQUE(source_surface, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    parent_task_id TEXT REFERENCES tasks(task_id),
                    state TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    created_seq INTEGER NOT NULL,
                    updated_seq INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    authority_class TEXT NOT NULL,
                    source_surface TEXT NOT NULL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    worker_id TEXT,
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    lease_expires_at TEXT,
                    last_heartbeat_at TEXT,
                    blocked_reason TEXT,
                    terminal_reason TEXT
                );

                CREATE TABLE IF NOT EXISTS tombstones (
                    task_id TEXT PRIMARY KEY,
                    created_seq INTEGER NOT NULL,
                    reason TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    lease_epoch INTEGER NOT NULL DEFAULT 0,
                    last_heartbeat_at TEXT,
                    lease_expires_at TEXT
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    task_id TEXT,
                    actor_surface TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    data_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS outbox (
                    outbox_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    envelope_json TEXT NOT NULL,
                    state TEXT NOT NULL DEFAULT 'PENDING',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS quarantine (
                    quarantine_seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT,
                    task_id TEXT,
                    reason TEXT NOT NULL,
                    envelope_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS steps (
                    operation_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL REFERENCES tasks(task_id),
                    lease_epoch INTEGER,
                    state TEXT NOT NULL,
                    authority_class TEXT NOT NULL,
                    scope_sha256 TEXT NOT NULL,
                    details_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS approvals (
                    operation_id TEXT PRIMARY KEY,
                    authority_class TEXT NOT NULL,
                    scope_sha256 TEXT NOT NULL,
                    approved_by TEXT NOT NULL,
                    approved_at TEXT NOT NULL,
                    expires_at TEXT
                );
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO metadata(key, value) VALUES('schema_version', '1')"
            )
            # Repair worker rows left ACTIVE by an interrupted or older core when no
            # task still holds that worker's lease. Task rows remain authoritative.
            conn.execute(
                """
                UPDATE workers SET state='IDLE', lease_expires_at=NULL
                WHERE state='ACTIVE' AND NOT EXISTS (
                    SELECT 1 FROM tasks
                    WHERE tasks.worker_id=workers.worker_id
                      AND tasks.state IN ('CLAIMED','RUNNING','CANCEL_REQUESTED')
                )
                """
            )

    def now(self) -> datetime:
        return self.now_fn()

    def _validate_envelope(self, envelope: Dict[str, Any]) -> Dict[str, Any]:
        required = (
            "protocol_version",
            "message_id",
            "idempotency_key",
            "task_id",
            "type",
            "source_surface",
        )
        missing = [key for key in required if key not in envelope]
        if missing:
            raise OrchestrationError("missing fields: " + ", ".join(missing))
        if envelope["protocol_version"] != PROTOCOL_VERSION:
            raise OrchestrationError("unsupported protocol_version")
        if envelope["type"] not in MESSAGE_TYPES:
            raise OrchestrationError("unsupported message type: %s" % envelope["type"])
        normalized = dict(envelope)
        normalized.setdefault("task_revision", 1)
        normalized.setdefault("expected_revision", None)
        normalized.setdefault("parent_task_id", None)
        normalized.setdefault("priority", 50)
        normalized.setdefault("authority_class", "local_reversible")
        normalized.setdefault("authored_at", None)
        normalized.setdefault("correlation_id", None)
        normalized.setdefault("payload", {})
        if normalized["authority_class"] not in AUTHORITY_CLASSES:
            raise OrchestrationError("invalid authority_class")
        if not isinstance(normalized["priority"], int) or not 0 <= normalized["priority"] <= 100:
            raise OrchestrationError("priority must be an integer from 0 to 100")
        if not isinstance(normalized["task_revision"], int) or normalized["task_revision"] < 1:
            raise OrchestrationError("task_revision must be a positive integer")
        supplied_hash = normalized.pop("content_sha256", None)
        calculated = content_hash(normalized)
        if supplied_hash and supplied_hash != calculated:
            raise OrchestrationError("content_sha256 mismatch")
        normalized["content_sha256"] = calculated
        return normalized

    def ingest(self, envelope: Dict[str, Any]) -> Dict[str, Any]:
        try:
            env = self._validate_envelope(envelope)
        except Exception as exc:
            raw = dict(envelope)
            self._quarantine(raw, "validation_error: %s" % exc)
            raise

        now_s = iso(self.now())
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            duplicate = conn.execute(
                """
                SELECT ingest_seq, message_id, task_id FROM messages
                WHERE message_id = ? OR (source_surface = ? AND idempotency_key = ?)
                ORDER BY ingest_seq LIMIT 1
                """,
                (env["message_id"], env["source_surface"], env["idempotency_key"]),
            ).fetchone()
            if duplicate:
                self._event(
                    conn,
                    "message.duplicate_ignored",
                    env["task_id"],
                    env["source_surface"],
                    {
                        "duplicate_message_id": env["message_id"],
                        "original_message_id": duplicate["message_id"],
                        "original_ingest_seq": duplicate["ingest_seq"],
                    },
                )
                conn.commit()
                result = {
                    "status": "duplicate",
                    "task_id": duplicate["task_id"],
                    "original_ingest_seq": duplicate["ingest_seq"],
                }
                self.sync_projections()
                return result

            cursor = conn.execute(
                """
                INSERT INTO messages(
                    message_id, source_surface, idempotency_key, task_id, type,
                    task_revision, expected_revision, priority, authority_class,
                    authored_at, received_at, correlation_id, content_sha256,
                    envelope_json, outcome
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'RECEIVED')
                """,
                (
                    env["message_id"],
                    env["source_surface"],
                    env["idempotency_key"],
                    env["task_id"],
                    env["type"],
                    env["task_revision"],
                    env["expected_revision"],
                    env["priority"],
                    env["authority_class"],
                    env["authored_at"],
                    now_s,
                    env["correlation_id"],
                    env["content_sha256"],
                    canonical_json(env),
                ),
            )
            ingest_seq = cursor.lastrowid
            try:
                result = self._apply_message(conn, env, ingest_seq)
                outcome = result["status"].upper()
            except RevisionConflict as exc:
                self._quarantine_in_tx(conn, env, "revision_conflict: %s" % exc)
                self._event(
                    conn,
                    "message.quarantined",
                    env["task_id"],
                    "work",
                    {"reason": str(exc), "message_id": env["message_id"]},
                )
                result = {"status": "quarantined", "reason": str(exc), "task_id": env["task_id"]}
                outcome = "QUARANTINED"
            except OrchestrationError as exc:
                self._quarantine_in_tx(conn, env, "apply_error: %s" % exc)
                self._event(
                    conn,
                    "message.quarantined",
                    env["task_id"],
                    "work",
                    {"reason": str(exc), "message_id": env["message_id"]},
                )
                result = {"status": "quarantined", "reason": str(exc), "task_id": env["task_id"]}
                outcome = "QUARANTINED"
            conn.execute(
                "UPDATE messages SET outcome = ? WHERE ingest_seq = ?", (outcome, ingest_seq)
            )
            conn.commit()
        self.sync_projections()
        result["ingest_seq"] = ingest_seq
        return result

    def _apply_message(
        self, conn: sqlite3.Connection, env: Dict[str, Any], ingest_seq: int
    ) -> Dict[str, Any]:
        kind = env["type"]
        task_id = env["task_id"]
        existing = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()

        if kind == "task.create":
            if existing:
                raise OrchestrationError("task already exists with a different message identity")
            tombstone = conn.execute(
                "SELECT * FROM tombstones WHERE task_id = ?", (task_id,)
            ).fetchone()
            state = "CANCELLED" if tombstone else "QUEUED"
            conn.execute(
                """
                INSERT INTO tasks(
                    task_id, parent_task_id, state, revision, priority,
                    created_seq, updated_seq, payload_json, authority_class,
                    source_surface, cancel_requested, terminal_reason
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    env["parent_task_id"],
                    state,
                    env["task_revision"],
                    env["priority"],
                    ingest_seq,
                    ingest_seq,
                    canonical_json(env["payload"]),
                    env["authority_class"],
                    env["source_surface"],
                    1 if tombstone else 0,
                    tombstone["reason"] if tombstone else None,
                ),
            )
            event_type = "task.delayed_create_cancelled" if tombstone else "task.created"
            self._event(conn, event_type, task_id, env["source_surface"], env)
            return {"status": "cancelled" if tombstone else "accepted", "task_id": task_id}

        if kind in {"task.cancel_requested", "task.stop_requested"} and not existing:
            conn.execute(
                "INSERT OR REPLACE INTO tombstones(task_id, created_seq, reason) VALUES(?, ?, ?)",
                (task_id, ingest_seq, kind),
            )
            self._event(conn, "task.cancel_tombstone_created", task_id, env["source_surface"], env)
            return {"status": "accepted", "task_id": task_id, "state": "TOMBSTONED"}

        if not existing:
            raise OrchestrationError("task does not exist")

        if kind in {"task.cancel_requested", "task.stop_requested"}:
            if existing["state"] in TERMINAL_STATES:
                self._event(
                    conn,
                    "task.cancel_ignored_terminal",
                    task_id,
                    env["source_surface"],
                    {"state": existing["state"], "message_id": env["message_id"]},
                )
                return {"status": "ignored", "task_id": task_id, "state": existing["state"]}
            revision = max(existing["revision"] + 1, env["task_revision"])
            conn.execute(
                """
                UPDATE tasks SET state='CANCEL_REQUESTED', cancel_requested=1,
                    revision=?, updated_seq=? WHERE task_id=?
                """,
                (revision, ingest_seq, task_id),
            )
            self._event(conn, "task.cancel_requested", task_id, env["source_surface"], env)
            return {"status": "accepted", "task_id": task_id, "state": "CANCEL_REQUESTED"}

        if existing["state"] in {"CANCEL_REQUESTED", "CANCELLED"}:
            self._event(
                conn,
                "message.ignored_cancel_dominates",
                task_id,
                env["source_surface"],
                {"message_id": env["message_id"], "type": kind},
            )
            return {"status": "ignored", "task_id": task_id, "state": existing["state"]}

        if kind in {"task.correct", "task.priority_changed"}:
            expected = env["expected_revision"]
            if expected is None:
                raise RevisionConflict("expected_revision is required")
            if expected != existing["revision"]:
                raise RevisionConflict(
                    "expected revision %s but current revision is %s"
                    % (expected, existing["revision"])
                )
            if env["task_revision"] != existing["revision"] + 1:
                raise RevisionConflict("task_revision must increment current revision by one")
            payload = json.loads(existing["payload_json"])
            if kind == "task.correct":
                payload = env["payload"]
            priority = env["priority"] if kind == "task.priority_changed" else existing["priority"]
            if "priority" in env and kind == "task.correct":
                priority = env["priority"]
            conn.execute(
                """
                UPDATE tasks SET revision=?, priority=?, payload_json=?,
                    authority_class=?, updated_seq=? WHERE task_id=?
                """,
                (
                    env["task_revision"],
                    priority,
                    canonical_json(payload),
                    env["authority_class"],
                    ingest_seq,
                    task_id,
                ),
            )
            self._event(conn, kind, task_id, env["source_surface"], env)
            return {
                "status": "accepted",
                "task_id": task_id,
                "revision": env["task_revision"],
                "priority": priority,
            }

        if kind == "question.answered":
            if existing["state"] != "BLOCKED":
                raise OrchestrationError("question answer requires a BLOCKED task")
            payload = json.loads(existing["payload_json"])
            payload.setdefault("answers", []).append(env["payload"])
            conn.execute(
                """
                UPDATE tasks SET state='QUEUED', blocked_reason=NULL,
                    payload_json=?, revision=?, updated_seq=?, worker_id=NULL,
                    lease_expires_at=NULL WHERE task_id=?
                """,
                (
                    canonical_json(payload),
                    max(existing["revision"] + 1, env["task_revision"]),
                    ingest_seq,
                    task_id,
                ),
            )
            self._event(conn, "question.answered", task_id, env["source_surface"], env)
            return {"status": "accepted", "task_id": task_id, "state": "QUEUED"}

        if kind in {"approval.granted", "approval.denied"}:
            self._event(conn, kind, task_id, env["source_surface"], env)
            return {"status": "accepted", "task_id": task_id}

        raise OrchestrationError("message type has no handler")

    def _event(
        self,
        conn: sqlite3.Connection,
        event_type: str,
        task_id: Optional[str],
        actor_surface: str,
        data: Dict[str, Any],
        publish: bool = True,
    ) -> str:
        event_id = str(uuid.uuid4())
        created_at = iso(self.now())
        conn.execute(
            """
            INSERT INTO events(event_id, event_type, task_id, actor_surface, created_at, data_json)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (event_id, event_type, task_id, actor_surface, created_at, canonical_json(data)),
        )
        if publish:
            envelope = {
                "protocol_version": PROTOCOL_VERSION,
                "event_id": event_id,
                "type": event_type,
                "task_id": task_id,
                "source_surface": actor_surface,
                "created_at": created_at,
                "payload": data,
            }
            conn.execute(
                """
                INSERT INTO outbox(event_id, envelope_json, state, attempts, created_at, updated_at)
                VALUES(?, ?, 'PENDING', 0, ?, ?)
                """,
                (event_id, canonical_json(envelope), created_at, created_at),
            )
        return event_id

    def _quarantine(self, envelope: Dict[str, Any], reason: str) -> None:
        with self.connect() as conn:
            self._quarantine_in_tx(conn, envelope, reason)
            self._event(
                conn,
                "message.quarantined",
                envelope.get("task_id"),
                "work",
                {"reason": reason, "message_id": envelope.get("message_id")},
            )
        self.sync_projections()

    def _quarantine_in_tx(
        self, conn: sqlite3.Connection, envelope: Dict[str, Any], reason: str
    ) -> None:
        conn.execute(
            """
            INSERT INTO quarantine(message_id, task_id, reason, envelope_json, created_at)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                envelope.get("message_id"),
                envelope.get("task_id"),
                reason,
                canonical_json(envelope),
                iso(self.now()),
            ),
        )

    def claim_next(self, worker_id: str, lease_seconds: int = 60) -> Optional[Dict[str, Any]]:
        now = self.now()
        expires = now + timedelta(seconds=lease_seconds)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            task = conn.execute(
                """
                SELECT * FROM tasks WHERE state='QUEUED'
                ORDER BY priority DESC, created_seq ASC LIMIT 1
                """
            ).fetchone()
            if not task:
                conn.commit()
                return None
            epoch = task["lease_epoch"] + 1
            conn.execute(
                """
                UPDATE tasks SET state='CLAIMED', worker_id=?, lease_epoch=?,
                    lease_expires_at=?, last_heartbeat_at=? WHERE task_id=? AND state='QUEUED'
                """,
                (worker_id, epoch, iso(expires), iso(now), task["task_id"]),
            )
            conn.execute(
                """
                INSERT INTO workers(worker_id, state, lease_epoch, last_heartbeat_at, lease_expires_at)
                VALUES(?, 'ACTIVE', ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET state='ACTIVE', lease_epoch=excluded.lease_epoch,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at
                """,
                (worker_id, epoch, iso(now), iso(expires)),
            )
            self._event(
                conn,
                "task.claimed",
                task["task_id"],
                "work",
                {"worker_id": worker_id, "lease_epoch": epoch, "lease_expires_at": iso(expires)},
            )
            conn.commit()
        self.sync_projections()
        return self.get_task(task["task_id"])

    def start_task(self, task_id: str, worker_id: str, lease_epoch: int) -> Dict[str, Any]:
        with self.connect() as conn:
            task = self._assert_lease(conn, task_id, worker_id, lease_epoch)
            if task["state"] != "CLAIMED":
                raise LeaseConflict("task must be CLAIMED before RUNNING")
            conn.execute("UPDATE tasks SET state='RUNNING' WHERE task_id=?", (task_id,))
            self._event(
                conn,
                "task.started",
                task_id,
                "work",
                {"worker_id": worker_id, "lease_epoch": lease_epoch},
            )
        self.sync_projections()
        return self.get_task(task_id)

    def heartbeat(
        self, worker_id: str, task_id: str, lease_epoch: int, lease_seconds: int = 60
    ) -> Dict[str, Any]:
        now = self.now()
        expires = now + timedelta(seconds=lease_seconds)
        with self.connect() as conn:
            self._assert_lease(conn, task_id, worker_id, lease_epoch)
            conn.execute(
                """
                UPDATE tasks SET last_heartbeat_at=?, lease_expires_at=? WHERE task_id=?
                """,
                (iso(now), iso(expires), task_id),
            )
            conn.execute(
                """
                UPDATE workers SET state='ACTIVE', last_heartbeat_at=?, lease_expires_at=?
                WHERE worker_id=?
                """,
                (iso(now), iso(expires), worker_id),
            )
            self._event(
                conn,
                "worker.heartbeat",
                task_id,
                "work",
                {"worker_id": worker_id, "lease_epoch": lease_epoch},
                publish=False,
            )
        self.sync_projections()
        return self.get_task(task_id)

    def checkpoint(
        self, task_id: str, worker_id: str, lease_epoch: int, observed_revision: int
    ) -> Dict[str, Any]:
        with self.connect() as conn:
            task = self._assert_lease(conn, task_id, worker_id, lease_epoch)
            result = {
                "task_id": task_id,
                "state": task["state"],
                "current_revision": task["revision"],
                "observed_revision": observed_revision,
                "revision_changed": task["revision"] != observed_revision,
                "cancel_requested": bool(task["cancel_requested"]),
                "priority": task["priority"],
                "payload": json.loads(task["payload_json"]),
            }
            self._event(conn, "task.checkpoint", task_id, "work", result, publish=False)
        self.sync_projections()
        return result

    def acknowledge_cancel(self, task_id: str, worker_id: str, lease_epoch: int) -> Dict[str, Any]:
        with self.connect() as conn:
            task = self._assert_lease(conn, task_id, worker_id, lease_epoch)
            if task["state"] != "CANCEL_REQUESTED":
                raise OrchestrationError("task is not awaiting cancellation acknowledgement")
            conn.execute(
                """
                UPDATE tasks SET state='CANCELLED', worker_id=NULL, lease_expires_at=NULL,
                    terminal_reason='cancel acknowledged at safe checkpoint' WHERE task_id=?
                """,
                (task_id,),
            )
            conn.execute(
                "UPDATE workers SET state='IDLE', lease_expires_at=NULL WHERE worker_id=?",
                (worker_id,),
            )
            self._event(
                conn,
                "task.cancelled",
                task_id,
                "work",
                {"worker_id": worker_id, "lease_epoch": lease_epoch},
            )
        self.sync_projections()
        return self.get_task(task_id)

    def block_task(
        self,
        task_id: str,
        worker_id: str,
        lease_epoch: int,
        reason: str,
        question_id: str,
        question: str,
    ) -> Dict[str, Any]:
        with self.connect() as conn:
            self._assert_lease(conn, task_id, worker_id, lease_epoch)
            conn.execute(
                """
                UPDATE tasks SET state='BLOCKED', blocked_reason=?, worker_id=NULL,
                    lease_expires_at=NULL WHERE task_id=?
                """,
                (reason, task_id),
            )
            conn.execute(
                "UPDATE workers SET state='IDLE', lease_expires_at=NULL WHERE worker_id=?",
                (worker_id,),
            )
            self._event(
                conn,
                "task.blocked",
                task_id,
                "work",
                {"reason": reason, "question_id": question_id, "question": question},
            )
            self._event(
                conn,
                "question.asked",
                task_id,
                "work",
                {"question_id": question_id, "question": question, "reason": reason},
            )
        self.sync_projections()
        return self.get_task(task_id)

    def complete_task(
        self,
        task_id: str,
        worker_id: str,
        lease_epoch: int,
        expected_revision: int,
        result: Dict[str, Any],
    ) -> Dict[str, Any]:
        with self.connect() as conn:
            task = self._assert_lease(conn, task_id, worker_id, lease_epoch)
            if task["revision"] != expected_revision:
                raise RevisionConflict(
                    "worker observed revision %s but current revision is %s"
                    % (expected_revision, task["revision"])
                )
            if task["state"] == "CANCEL_REQUESTED":
                raise OrchestrationError("cancel request dominates completion")
            children = conn.execute(
                "SELECT task_id, state FROM tasks WHERE parent_task_id=?", (task_id,)
            ).fetchall()
            incomplete = [row["task_id"] for row in children if row["state"] != "SUCCEEDED"]
            if incomplete:
                raise OrchestrationError("mission has incomplete subtasks: %s" % ", ".join(incomplete))
            conn.execute(
                """
                UPDATE tasks SET state='SUCCEEDED', worker_id=NULL, lease_expires_at=NULL,
                    terminal_reason='success' WHERE task_id=?
                """,
                (task_id,),
            )
            conn.execute(
                "UPDATE workers SET state='IDLE', lease_expires_at=NULL WHERE worker_id=?",
                (worker_id,),
            )
            event_type = "subtask.completed" if task["parent_task_id"] else "task.completed"
            self._event(conn, event_type, task_id, "work", {"result": result})
        self.sync_projections()
        return self.get_task(task_id)

    def sweep_stale(self) -> List[str]:
        now = self.now()
        stale: List[str] = []
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                """
                SELECT * FROM tasks WHERE state IN ('CLAIMED','RUNNING','CANCEL_REQUESTED')
                    AND lease_expires_at IS NOT NULL
                """
            ).fetchall()
            for task in rows:
                if parse_iso(task["lease_expires_at"]) >= now:
                    continue
                stale.append(task["task_id"])
                conn.execute(
                    """
                    UPDATE tasks SET state='ORPHANED', worker_id=NULL, lease_expires_at=NULL,
                        terminal_reason='worker lease expired' WHERE task_id=?
                    """,
                    (task["task_id"],),
                )
                conn.execute(
                    "UPDATE workers SET state='STALE' WHERE worker_id=?", (task["worker_id"],)
                )
                self._event(
                    conn,
                    "worker.stale",
                    task["task_id"],
                    "work",
                    {
                        "worker_id": task["worker_id"],
                        "last_heartbeat_at": task["last_heartbeat_at"],
                        "expired_lease_epoch": task["lease_epoch"],
                    },
                )
            conn.commit()
        self.sync_projections()
        return stale

    def recover_orphaned(
        self, task_id: str, worker_id: str, lease_seconds: int = 60
    ) -> Dict[str, Any]:
        now = self.now()
        expires = now + timedelta(seconds=lease_seconds)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not task or task["state"] != "ORPHANED":
                raise OrchestrationError("task is not ORPHANED")
            unknown = conn.execute(
                "SELECT operation_id FROM steps WHERE task_id=? AND state='OUTCOME_UNKNOWN'",
                (task_id,),
            ).fetchall()
            if unknown:
                raise OrchestrationError(
                    "manual reconciliation required for operations: %s"
                    % ", ".join(row["operation_id"] for row in unknown)
                )
            epoch = task["lease_epoch"] + 1
            conn.execute(
                """
                UPDATE tasks SET state='CLAIMED', worker_id=?, lease_epoch=?,
                    lease_expires_at=?, last_heartbeat_at=?, terminal_reason=NULL
                WHERE task_id=?
                """,
                (worker_id, epoch, iso(expires), iso(now), task_id),
            )
            conn.execute(
                """
                INSERT INTO workers(worker_id, state, lease_epoch, last_heartbeat_at, lease_expires_at)
                VALUES(?, 'ACTIVE', ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET state='ACTIVE', lease_epoch=excluded.lease_epoch,
                    last_heartbeat_at=excluded.last_heartbeat_at,
                    lease_expires_at=excluded.lease_expires_at
                """,
                (worker_id, epoch, iso(now), iso(expires)),
            )
            self._event(
                conn,
                "task.recovered",
                task_id,
                "work",
                {"worker_id": worker_id, "lease_epoch": epoch},
            )
            conn.commit()
        self.sync_projections()
        return self.get_task(task_id)

    def approve_operation(
        self,
        operation_id: str,
        authority_class: str,
        details: Dict[str, Any],
        approved_by: str,
        expires_at: Optional[str] = None,
    ) -> None:
        if authority_class not in AUTHORITY_CLASSES:
            raise OrchestrationError("invalid authority_class")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO approvals(
                    operation_id, authority_class, scope_sha256, approved_by, approved_at, expires_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    authority_class,
                    content_hash(details),
                    approved_by,
                    iso(self.now()),
                    expires_at,
                ),
            )

    def record_step_intent(
        self,
        operation_id: str,
        task_id: str,
        lease_epoch: int,
        authority_class: str,
        details: Dict[str, Any],
    ) -> Dict[str, Any]:
        if authority_class not in AUTHORITY_CLASSES:
            raise OrchestrationError("invalid authority_class")
        scope = content_hash(details)
        with self.connect() as conn:
            task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not task:
                raise OrchestrationError("task does not exist")
            if task["lease_epoch"] != lease_epoch:
                raise LeaseConflict("stale lease epoch")
            if authority_class not in AUTO_AUTHORITY:
                approval = conn.execute(
                    "SELECT * FROM approvals WHERE operation_id=?", (operation_id,)
                ).fetchone()
                if not approval or approval["scope_sha256"] != scope:
                    raise AuthorityRequired("exact operation approval is required")
                if approval["expires_at"] and parse_iso(approval["expires_at"]) < self.now():
                    raise AuthorityRequired("operation approval expired")
            now_s = iso(self.now())
            conn.execute(
                """
                INSERT INTO steps(operation_id, task_id, lease_epoch, state, authority_class,
                    scope_sha256, details_json, created_at, updated_at)
                VALUES(?, ?, ?, 'INTENT_RECORDED', ?, ?, ?, ?, ?)
                """,
                (
                    operation_id,
                    task_id,
                    lease_epoch,
                    authority_class,
                    scope,
                    canonical_json(details),
                    now_s,
                    now_s,
                ),
            )
            self._event(
                conn,
                "step.intent_recorded",
                task_id,
                "work",
                {"operation_id": operation_id, "authority_class": authority_class, "details": details},
            )
        self.sync_projections()
        return self.get_step(operation_id)

    def mark_step_outcome(self, operation_id: str, state: str) -> Dict[str, Any]:
        if state not in {"COMPLETED", "FAILED", "OUTCOME_UNKNOWN"}:
            raise OrchestrationError("invalid step outcome")
        with self.connect() as conn:
            step = conn.execute("SELECT * FROM steps WHERE operation_id=?", (operation_id,)).fetchone()
            if not step:
                raise OrchestrationError("operation does not exist")
            conn.execute(
                "UPDATE steps SET state=?, updated_at=? WHERE operation_id=?",
                (state, iso(self.now()), operation_id),
            )
            self._event(
                conn,
                "step.%s" % state.lower(),
                step["task_id"],
                "work",
                {"operation_id": operation_id},
            )
        self.sync_projections()
        return self.get_step(operation_id)

    def _assert_lease(
        self, conn: sqlite3.Connection, task_id: str, worker_id: str, lease_epoch: int
    ) -> sqlite3.Row:
        task = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        if not task:
            raise LeaseConflict("task does not exist")
        if task["worker_id"] != worker_id or task["lease_epoch"] != lease_epoch:
            raise LeaseConflict("worker does not hold the current fenced lease")
        return task

    def get_task(self, task_id: str) -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not row:
                raise OrchestrationError("task does not exist")
            result = dict(row)
            result["payload"] = json.loads(result.pop("payload_json"))
            result["cancel_requested"] = bool(result["cancel_requested"])
            return result

    def get_step(self, operation_id: str) -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM steps WHERE operation_id=?", (operation_id,)).fetchone()
            if not row:
                raise OrchestrationError("operation does not exist")
            result = dict(row)
            result["details"] = json.loads(result.pop("details_json"))
            return result

    def list_tasks(self) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM tasks ORDER BY priority DESC, created_seq ASC"
            ).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["payload"] = json.loads(item.pop("payload_json"))
                item["cancel_requested"] = bool(item["cancel_requested"])
                result.append(item)
            return result

    def list_events(self) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM events ORDER BY event_seq").fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["data"] = json.loads(item.pop("data_json"))
                result.append(item)
            return result

    def list_outbox(self, state: Optional[str] = None) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            if state:
                rows = conn.execute(
                    "SELECT * FROM outbox WHERE state=? ORDER BY outbox_seq", (state,)
                ).fetchall()
            else:
                rows = conn.execute("SELECT * FROM outbox ORDER BY outbox_seq").fetchall()
            result = []
            for row in rows:
                item = dict(row)
                item["envelope"] = json.loads(item.pop("envelope_json"))
                result.append(item)
            return result

    def mark_outbox_sent(self, event_id: str) -> Dict[str, Any]:
        """Mark one durable outbox item sent without treating it as acknowledged."""
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM outbox WHERE event_id=?", (event_id,)).fetchone()
            if not row:
                raise OrchestrationError("outbox event does not exist")
            conn.execute(
                """
                UPDATE outbox SET state=CASE WHEN state='ACKED' THEN state ELSE 'SENT' END,
                    attempts=attempts+1, updated_at=? WHERE event_id=?
                """,
                (iso(self.now()), event_id),
            )
        self.sync_projections()
        return next(item for item in self.list_outbox() if item["event_id"] == event_id)

    def mark_outbox_acked(self, event_id: str) -> Dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM outbox WHERE event_id=?", (event_id,)).fetchone()
            if not row:
                raise OrchestrationError("outbox event does not exist")
            conn.execute(
                "UPDATE outbox SET state='ACKED', updated_at=? WHERE event_id=?",
                (iso(self.now()), event_id),
            )
        self.sync_projections()
        return next(item for item in self.list_outbox() if item["event_id"] == event_id)

    def status(self) -> Dict[str, Any]:
        with self.connect() as conn:
            state_counts = {
                row["state"]: row["count"]
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM tasks GROUP BY state"
                ).fetchall()
            }
            last_event = conn.execute(
                "SELECT event_seq, created_at FROM events ORDER BY event_seq DESC LIMIT 1"
            ).fetchone()
            pending_outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE state='PENDING'"
            ).fetchone()["count"]
            outbox_counts = {
                row["state"]: row["count"]
                for row in conn.execute(
                    "SELECT state, COUNT(*) AS count FROM outbox GROUP BY state"
                ).fetchall()
            }
            unacknowledged_outbox = conn.execute(
                "SELECT COUNT(*) AS count FROM outbox WHERE state!='ACKED'"
            ).fetchone()["count"]
            stale_workers = [
                row["worker_id"]
                for row in conn.execute(
                    "SELECT worker_id FROM workers WHERE state='STALE' ORDER BY worker_id"
                ).fetchall()
            ]
            return {
                "protocol_version": PROTOCOL_VERSION,
                "generated_at": iso(self.now()),
                "database": str(self.db_path),
                "task_counts": state_counts,
                "last_event_seq": last_event["event_seq"] if last_event else 0,
                "last_event_at": last_event["created_at"] if last_event else None,
                "pending_outbox": pending_outbox,
                "unacknowledged_outbox": unacknowledged_outbox,
                "outbox_counts": outbox_counts,
                "stale_workers": stale_workers,
            }

    def sync_projections(self) -> None:
        self._atomic_write(
            self.events_path,
            "".join(canonical_json(event) + "\n" for event in self.list_events()),
        )
        self._atomic_write(self.status_path, json.dumps(self.status(), indent=2, sort_keys=True) + "\n")

    @staticmethod
    def _atomic_write(path: Path, data: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)


def load_envelope(args: argparse.Namespace) -> Dict[str, Any]:
    if args.json:
        return json.loads(args.json)
    with open(args.file, "r", encoding="utf-8") as handle:
        return json.load(handle)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(os.environ.get("P42_ORCHESTRATION_ROOT", Path(__file__).resolve().parent)),
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    ingest = sub.add_parser("ingest")
    source = ingest.add_mutually_exclusive_group(required=True)
    source.add_argument("--json")
    source.add_argument("--file", type=Path)
    claim = sub.add_parser("claim")
    claim.add_argument("--worker", required=True)
    claim.add_argument("--lease-seconds", type=int, default=60)
    start = sub.add_parser("start")
    start.add_argument("task_id")
    start.add_argument("--worker", required=True)
    start.add_argument("--lease-epoch", required=True, type=int)
    heartbeat = sub.add_parser("heartbeat")
    heartbeat.add_argument("task_id")
    heartbeat.add_argument("--worker", required=True)
    heartbeat.add_argument("--lease-epoch", required=True, type=int)
    heartbeat.add_argument("--lease-seconds", type=int, default=60)
    checkpoint = sub.add_parser("checkpoint")
    checkpoint.add_argument("task_id")
    checkpoint.add_argument("--worker", required=True)
    checkpoint.add_argument("--lease-epoch", required=True, type=int)
    checkpoint.add_argument("--observed-revision", required=True, type=int)
    block = sub.add_parser("block")
    block.add_argument("task_id")
    block.add_argument("--worker", required=True)
    block.add_argument("--lease-epoch", required=True, type=int)
    block.add_argument("--reason", required=True)
    block.add_argument("--question-id", required=True)
    block.add_argument("--question", required=True)
    cancel = sub.add_parser("ack-cancel")
    cancel.add_argument("task_id")
    cancel.add_argument("--worker", required=True)
    cancel.add_argument("--lease-epoch", required=True, type=int)
    complete = sub.add_parser("complete")
    complete.add_argument("task_id")
    complete.add_argument("--worker", required=True)
    complete.add_argument("--lease-epoch", required=True, type=int)
    complete.add_argument("--expected-revision", required=True, type=int)
    complete.add_argument("--result-json", default="{}")
    sub.add_parser("sweep-stale")
    recover = sub.add_parser("recover")
    recover.add_argument("task_id")
    recover.add_argument("--worker", required=True)
    recover.add_argument("--lease-seconds", type=int, default=60)
    task = sub.add_parser("task")
    task.add_argument("task_id")
    sub.add_parser("tasks")
    sub.add_parser("events")
    sub.add_parser("outbox")
    sub.add_parser("status")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    orch = Orchestrator(args.root)
    try:
        if args.command == "init":
            value: Any = orch.status()
        elif args.command == "ingest":
            value = orch.ingest(load_envelope(args))
        elif args.command == "claim":
            value = orch.claim_next(args.worker, args.lease_seconds)
        elif args.command == "start":
            value = orch.start_task(args.task_id, args.worker, args.lease_epoch)
        elif args.command == "heartbeat":
            value = orch.heartbeat(
                args.worker, args.task_id, args.lease_epoch, args.lease_seconds
            )
        elif args.command == "checkpoint":
            value = orch.checkpoint(
                args.task_id, args.worker, args.lease_epoch, args.observed_revision
            )
        elif args.command == "block":
            value = orch.block_task(
                args.task_id,
                args.worker,
                args.lease_epoch,
                args.reason,
                args.question_id,
                args.question,
            )
        elif args.command == "ack-cancel":
            value = orch.acknowledge_cancel(args.task_id, args.worker, args.lease_epoch)
        elif args.command == "complete":
            value = orch.complete_task(
                args.task_id,
                args.worker,
                args.lease_epoch,
                args.expected_revision,
                json.loads(args.result_json),
            )
        elif args.command == "sweep-stale":
            value = {"stale_tasks": orch.sweep_stale()}
        elif args.command == "recover":
            value = orch.recover_orphaned(args.task_id, args.worker, args.lease_seconds)
        elif args.command == "task":
            value = orch.get_task(args.task_id)
        elif args.command == "tasks":
            value = orch.list_tasks()
        elif args.command == "events":
            value = orch.list_events()
        elif args.command == "outbox":
            value = orch.list_outbox()
        elif args.command == "status":
            value = orch.status()
        else:
            parser.error("unknown command")
            return 2
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except OrchestrationError as exc:
        print(json.dumps({"error": type(exc).__name__, "message": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
