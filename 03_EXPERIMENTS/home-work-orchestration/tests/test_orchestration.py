import json
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from p42_orchestrator import (  # noqa: E402
    AuthorityRequired,
    LeaseConflict,
    OrchestrationError,
    Orchestrator,
    RevisionConflict,
    iso,
)


class FakeClock:
    def __init__(self):
        self.value = datetime(2026, 10, 25, 5, 0, tzinfo=timezone.utc)

    def now(self):
        return self.value

    def advance(self, seconds):
        self.value += timedelta(seconds=seconds)


class OrchestratorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.clock = FakeClock()
        self.orch = Orchestrator(self.root, now_fn=self.clock.now)
        self.counter = 0

    def tearDown(self):
        self.temp.cleanup()

    def env(
        self,
        task_id,
        kind="task.create",
        *,
        revision=1,
        expected=None,
        priority=50,
        payload=None,
        parent=None,
        message_id=None,
        idempotency_key=None,
        authority="local_reversible",
    ):
        self.counter += 1
        token = str(self.counter)
        return {
            "protocol_version": 1,
            "message_id": message_id or "msg-" + token,
            "idempotency_key": idempotency_key or "idem-" + token,
            "task_id": task_id,
            "parent_task_id": parent,
            "type": kind,
            "task_revision": revision,
            "expected_revision": expected,
            "source_surface": "home",
            "priority": priority,
            "authority_class": authority,
            "payload": payload or {"instruction": "test"},
        }

    def create_running(self, task_id="T1", priority=50, worker="worker-a"):
        self.orch.ingest(self.env(task_id, priority=priority))
        claimed = self.orch.claim_next(worker, lease_seconds=30)
        self.assertEqual(task_id, claimed["task_id"])
        self.orch.start_task(task_id, worker, claimed["lease_epoch"])
        return claimed["lease_epoch"]

    def test_home_task_arrives_while_work_active_and_is_claimed(self):
        result = self.orch.ingest(
            self.env("HOME-A1", payload={"instruction": "record nonce 7K4M"})
        )
        self.assertEqual("accepted", result["status"])
        claimed = self.orch.claim_next("active-work")
        self.assertEqual("HOME-A1", claimed["task_id"])
        event_types = [event["event_type"] for event in self.orch.list_events()]
        self.assertIn("task.created", event_types)
        self.assertIn("task.claimed", event_types)

    def test_priority_change_mid_task_is_seen_at_checkpoint(self):
        epoch = self.create_running("PRIORITY")
        update = self.env(
            "PRIORITY",
            "task.priority_changed",
            revision=2,
            expected=1,
            priority=95,
        )
        self.orch.ingest(update)
        checkpoint = self.orch.checkpoint("PRIORITY", "worker-a", epoch, observed_revision=1)
        self.assertTrue(checkpoint["revision_changed"])
        self.assertEqual(95, checkpoint["priority"])
        self.assertEqual(2, checkpoint["current_revision"])

    def test_correction_preserves_history_and_fences_stale_completion(self):
        epoch = self.create_running("CORRECT")
        self.orch.ingest(
            self.env(
                "CORRECT",
                "task.correct",
                revision=2,
                expected=1,
                payload={"instruction": "corrected plan"},
            )
        )
        with self.assertRaises(RevisionConflict):
            self.orch.complete_task("CORRECT", "worker-a", epoch, 1, {"wrong": True})
        task = self.orch.complete_task("CORRECT", "worker-a", epoch, 2, {"ok": True})
        self.assertEqual("SUCCEEDED", task["state"])
        self.assertEqual("corrected plan", task["payload"]["instruction"])
        messages = []
        with self.orch.connect() as conn:
            messages = conn.execute(
                "SELECT type, envelope_json FROM messages WHERE task_id=? ORDER BY ingest_seq",
                ("CORRECT",),
            ).fetchall()
        self.assertEqual(["task.create", "task.correct"], [row["type"] for row in messages])

    def test_cancellation_dominates_and_stops_at_safe_checkpoint(self):
        epoch = self.create_running("CANCEL")
        self.orch.ingest(
            self.env("CANCEL", "task.cancel_requested", revision=2, expected=1)
        )
        checkpoint = self.orch.checkpoint("CANCEL", "worker-a", epoch, observed_revision=1)
        self.assertTrue(checkpoint["cancel_requested"])
        with self.assertRaises(OrchestrationError):
            self.orch.complete_task("CANCEL", "worker-a", epoch, 2, {"ignored": True})
        cancelled = self.orch.acknowledge_cancel("CANCEL", "worker-a", epoch)
        self.assertEqual("CANCELLED", cancelled["state"])

    def test_blocker_question_and_answer_resume_same_task(self):
        epoch = self.create_running("BLOCK")
        blocked = self.orch.block_task(
            "BLOCK",
            "worker-a",
            epoch,
            "path is read-only",
            "Q-BLOCK-1",
            "Use the writable workspace?",
        )
        self.assertEqual("BLOCKED", blocked["state"])
        with self.orch.connect() as conn:
            worker = conn.execute(
                "SELECT state, lease_expires_at FROM workers WHERE worker_id='worker-a'"
            ).fetchone()
        self.assertEqual("IDLE", worker["state"])
        self.assertIsNone(worker["lease_expires_at"])
        with self.orch.connect() as conn:
            conn.execute(
                "UPDATE workers SET state='ACTIVE', lease_expires_at=? WHERE worker_id='worker-a'",
                (iso(self.clock.now()),),
            )
        restarted = Orchestrator(self.root, now_fn=self.clock.now)
        with restarted.connect() as conn:
            repaired = conn.execute(
                "SELECT state, lease_expires_at FROM workers WHERE worker_id='worker-a'"
            ).fetchone()
        self.assertEqual("IDLE", repaired["state"])
        self.assertIsNone(repaired["lease_expires_at"])
        out_types = [item["envelope"]["type"] for item in self.orch.list_outbox()]
        self.assertIn("question.asked", out_types)
        resumed = self.orch.ingest(
            self.env(
                "BLOCK",
                "question.answered",
                revision=2,
                payload={"question_id": "Q-BLOCK-1", "answer": "yes"},
            )
        )
        self.assertEqual("accepted", resumed["status"])
        self.assertEqual("QUEUED", self.orch.get_task("BLOCK")["state"])

    def test_duplicate_delivery_executes_once_and_is_logged(self):
        envelope = self.env(
            "DUP",
            message_id="same-message",
            idempotency_key="same-key",
        )
        self.orch.ingest(envelope)
        duplicate = self.orch.ingest(dict(envelope))
        self.assertEqual("duplicate", duplicate["status"])
        self.assertEqual(1, len(self.orch.list_tasks()))
        event_types = [event["event_type"] for event in self.orch.list_events()]
        self.assertEqual(1, event_types.count("message.duplicate_ignored"))

    def test_worker_interruption_restart_stale_lease_and_recovery(self):
        self.orch.ingest(self.env("RESTART"))
        claimed = self.orch.claim_next("old-worker", lease_seconds=5)
        old_epoch = claimed["lease_epoch"]
        self.orch.start_task("RESTART", "old-worker", old_epoch)
        self.clock.advance(6)

        restarted = Orchestrator(self.root, now_fn=self.clock.now)
        self.assertEqual(["RESTART"], restarted.sweep_stale())
        self.assertEqual("ORPHANED", restarted.get_task("RESTART")["state"])
        recovered = restarted.recover_orphaned("RESTART", "new-worker")
        self.assertGreater(recovered["lease_epoch"], old_epoch)
        with self.assertRaises(LeaseConflict):
            restarted.heartbeat("old-worker", "RESTART", old_epoch)

    def test_multiple_tasks_use_priority_then_fifo(self):
        self.orch.ingest(self.env("LOW", priority=10))
        self.orch.ingest(self.env("HIGH-1", priority=90))
        self.orch.ingest(self.env("HIGH-2", priority=90))
        first = self.orch.claim_next("worker-1")
        second = self.orch.claim_next("worker-2")
        third = self.orch.claim_next("worker-3")
        self.assertEqual(["HIGH-1", "HIGH-2", "LOW"], [first["task_id"], second["task_id"], third["task_id"]])

    def test_subtask_completion_does_not_complete_mission(self):
        self.orch.ingest(self.env("MISSION"))
        self.orch.ingest(self.env("CHILD-1", parent="MISSION", priority=90))
        self.orch.ingest(self.env("CHILD-2", parent="MISSION", priority=80))
        child = self.orch.claim_next("child-worker")
        self.assertEqual("CHILD-1", child["task_id"])
        self.orch.start_task("CHILD-1", "child-worker", child["lease_epoch"])
        self.orch.complete_task("CHILD-1", "child-worker", child["lease_epoch"], 1, {"done": 1})
        self.assertEqual("QUEUED", self.orch.get_task("MISSION")["state"])
        parent_event_types = [
            event["event_type"] for event in self.orch.list_events() if event["task_id"] == "MISSION"
        ]
        self.assertNotIn("task.completed", parent_event_types)

    def test_stale_heartbeat_is_visible_in_status(self):
        self.orch.ingest(self.env("STALE"))
        self.orch.claim_next("sleepy-worker", lease_seconds=2)
        self.clock.advance(3)
        self.orch.sweep_stale()
        status = self.orch.status()
        self.assertIn("sleepy-worker", status["stale_workers"])
        self.assertEqual(1, status["task_counts"]["ORPHANED"])

    def test_cancel_before_create_forms_tombstone(self):
        self.orch.ingest(self.env("LATE", "task.cancel_requested"))
        created = self.orch.ingest(self.env("LATE", "task.create", revision=1))
        self.assertEqual("cancelled", created["status"])
        self.assertEqual("CANCELLED", self.orch.get_task("LATE")["state"])

    def test_two_workers_cannot_claim_one_task(self):
        self.orch.ingest(self.env("ONE"))
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda name: self.orch.claim_next(name), ["w1", "w2"]))
        claimed = [result for result in results if result is not None]
        self.assertEqual(1, len(claimed))
        self.assertEqual("ONE", claimed[0]["task_id"])

    def test_exact_approval_is_required_for_consequential_step(self):
        epoch = self.create_running("AUTH")
        details = {"action": "publish report", "destination": "public repo"}
        with self.assertRaises(AuthorityRequired):
            self.orch.record_step_intent(
                "OP-1", "AUTH", epoch, "public_or_irreversible", details
            )
        self.orch.approve_operation(
            "OP-1",
            "public_or_irreversible",
            details,
            approved_by="authorized-controller",
        )
        step = self.orch.record_step_intent(
            "OP-1", "AUTH", epoch, "public_or_irreversible", details
        )
        self.assertEqual("INTENT_RECORDED", step["state"])

    def test_unknown_side_effect_blocks_automatic_recovery(self):
        epoch = self.create_running("UNCERTAIN")
        self.orch.record_step_intent(
            "OP-UNCERTAIN",
            "UNCERTAIN",
            epoch,
            "local_reversible",
            {"action": "write file"},
        )
        self.orch.mark_step_outcome("OP-UNCERTAIN", "OUTCOME_UNKNOWN")
        self.clock.advance(31)
        self.orch.sweep_stale()
        with self.assertRaises(OrchestrationError):
            self.orch.recover_orphaned("UNCERTAIN", "recovery-worker")

    def test_projections_are_valid_and_inspectable(self):
        self.orch.ingest(self.env("PROJECTION"))
        status = json.loads((self.root / "snapshots" / "status.json").read_text())
        lines = (self.root / "logs" / "events.jsonl").read_text().splitlines()
        self.assertEqual(1, status["task_counts"]["QUEUED"])
        self.assertEqual(1, status["unacknowledged_outbox"])
        self.assertEqual(1, status["outbox_counts"]["PENDING"])
        self.assertTrue(lines)
        self.assertEqual("task.created", json.loads(lines[0])["event_type"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
