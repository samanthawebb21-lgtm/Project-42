import json
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from file_adapter import FileAdapter, atomic_write  # noqa: E402
from p42_orchestrator import Orchestrator  # noqa: E402


class FileAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.orch = Orchestrator(self.root)
        self.adapter = FileAdapter(self.root, self.orch)

    def tearDown(self):
        self.temp.cleanup()

    def envelope(self, message_id="file-1", idempotency_key="file-key-1"):
        return {
            "protocol_version": 1,
            "message_id": message_id,
            "idempotency_key": idempotency_key,
            "task_id": "FILE-TASK",
            "type": "task.create",
            "task_revision": 1,
            "source_surface": "home",
            "priority": 50,
            "authority_class": "local_reversible",
            "payload": {"instruction": "filesystem adapter probe"},
        }

    def write_inbox(self, name, value):
        destination = self.adapter.incoming / name
        atomic_write(destination, json.dumps(value))
        return destination

    def test_valid_file_is_ingested_and_preserved_as_processed(self):
        source = self.write_inbox("one.json", self.envelope())
        results = self.adapter.scan_inbox_once()
        self.assertFalse(source.exists())
        self.assertEqual("accepted", results[0]["status"])
        self.assertTrue((self.adapter.processed / "one.json").exists())
        self.assertEqual("QUEUED", self.orch.get_task("FILE-TASK")["state"])

    def test_duplicate_file_is_deduplicated(self):
        envelope = self.envelope()
        self.write_inbox("one.json", envelope)
        self.adapter.scan_inbox_once()
        self.write_inbox("two.json", envelope)
        results = self.adapter.scan_inbox_once()
        self.assertEqual("duplicate", results[0]["status"])
        self.assertEqual(1, len(self.orch.list_tasks()))

    def test_malformed_file_is_preserved_in_quarantine(self):
        source = self.adapter.incoming / "bad.json"
        atomic_write(source, "{not-json")
        results = self.adapter.scan_inbox_once()
        self.assertEqual("quarantined", results[0]["status"])
        self.assertTrue((self.adapter.invalid / "bad.json").exists())
        self.assertTrue((self.adapter.invalid / "bad.json.reason.json").exists())

    def test_outbox_retry_reuses_stable_file_and_ack_stops_retry(self):
        self.orch.ingest(self.envelope())
        first = self.adapter.publish_outbox_once()
        self.assertTrue(first)
        event_id = first[0]["event_id"]
        file_path = Path(first[0]["file"])
        first_bytes = file_path.read_bytes()
        second = self.adapter.publish_outbox_once()
        self.assertTrue(any(item["event_id"] == event_id for item in second))
        self.assertEqual(first_bytes, file_path.read_bytes())
        self.adapter.acknowledge(event_id)
        third = self.adapter.publish_outbox_once()
        self.assertFalse(any(item["event_id"] == event_id for item in third))


if __name__ == "__main__":
    unittest.main(verbosity=2)
