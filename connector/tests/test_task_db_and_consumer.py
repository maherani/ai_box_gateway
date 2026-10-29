# -*- coding: utf-8 -*-
"""Unit tests for Task DB and Task Consumer in AI Box."""

import json
import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from task_db import (
    TaskDB,
    TASK_STATUS_PENDING,
    TASK_STATUS_PROCESSING,
    TASK_STATUS_RETRY_WAIT,
    TASK_STATUS_INDEX_VERIFY_PENDING,
    TASK_STATUS_INDEXED,
    TASK_STATUS_SUPERSEDED,
    TASK_STATUS_NEEDS_REVIEW,
)
from consumer import TaskConsumer
from destination_adapter import DeliveryContract, DeliveryResult, DeliveryStatus


class MockAMQPChannel:
    def __init__(self):
        self.acked = []
        self.nacked = []

    def basic_ack(self, delivery_tag: int) -> None:
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None:
        self.nacked.append((delivery_tag, requeue))


class MockDestinationAdapter:
    def __init__(self, status: DeliveryStatus = DeliveryStatus.SUCCESS, message: str = "Success"):
        self.status = status
        self.message = message
        self.contracts = []
        self.removed_documents = []
        self.workspace_slug = "ai-box-test"

    def resolve_or_create_group_workspace(self, group_name: str) -> str:
        clean = group_name.strip().lower().replace(" ", "-")
        return f"{clean}-workspace"

    def deliver(self, contract: DeliveryContract) -> DeliveryResult:
        self.contracts.append(contract)
        target_ws = contract.workspace_slug or self.workspace_slug
        doc_loc = f"custom-documents/arch_{contract.source_file_id}_{contract.content_hash}_{contract.file_name}.json"
        return DeliveryResult(status=self.status, message=self.message, doc_location=doc_loc, workspace_slug=target_ws)

    def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
        self.removed_documents.append((doc_location, workspace_slug))
        return True


class TestTaskDBAndConsumer(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_tasks.db")
        self.task_db = TaskDB(self.db_path)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_task_db_wal_and_sync_full(self):
        """Verify Task DB enforces WAL mode and FULL synchronous fsync commit guarantee."""
        with self.task_db._get_connection() as conn:
            mode = conn.execute("PRAGMA journal_mode;").fetchone()[0]
            sync = conn.execute("PRAGMA synchronous;").fetchone()[0]
            self.assertEqual(mode.upper(), "WAL")
            self.assertEqual(sync, 2)

    def test_task_db_deduplication(self):
        """Duplicate event_id is safely ignored without creating duplicate tasks."""
        event = {
            "event_id": "evt_1001",
            "file_id": 50,
            "file_name": "memo.txt",
            "file_path": "/files/memo.txt",
            "owner_uid": "user1",
            "sha256": "abc123hash",
        }
        is_new1, task_id1 = self.task_db.record_task(event)
        self.assertTrue(is_new1)
        self.assertIsNotNone(task_id1)

        is_new2, task_id2 = self.task_db.record_task(event)
        self.assertFalse(is_new2)
        self.assertEqual(task_id1, task_id2)

    def test_task_db_superseding_older_events(self):
        """New event for the same file_id marks older pending tasks as SUPERSEDED."""
        old_event = {
            "event_id": "evt_v1",
            "file_id": 200,
            "file_name": "report.md",
            "sha256": "v1hash",
        }
        new_event = {
            "event_id": "evt_v2",
            "file_id": 200,
            "file_name": "report.md",
            "sha256": "v2hash",
        }
        _, id1 = self.task_db.record_task(old_event)
        _, id2 = self.task_db.record_task(new_event)

        task1 = self.task_db.get_task(id1)
        task2 = self.task_db.get_task(id2)

        self.assertEqual(task1["status"], TASK_STATUS_SUPERSEDED)
        self.assertEqual(task2["status"], TASK_STATUS_PENDING)

    def test_task_db_lease_claiming_and_fencing(self):
        """Verify atomic task lease claiming and fencing token protection."""
        event = {
            "event_id": "evt_claim_1",
            "file_id": 300,
            "file_name": "data.csv",
            "sha256": "csvhash",
        }
        _, task_id = self.task_db.record_task(event)

        # Worker A claims
        claimed = self.task_db.claim_task("worker-A", lease_duration_seconds=300)
        self.assertIsNotNone(claimed)
        self.assertEqual(claimed["id"], task_id)
        self.assertEqual(claimed["status"], TASK_STATUS_PROCESSING)
        self.assertEqual(claimed["lease_owner"], "worker-A")

        # Worker B tries to claim - queue is empty (task is held under active lease)
        claimed_b = self.task_db.claim_task("worker-B", lease_duration_seconds=300)
        self.assertIsNone(claimed_b)

        # Worker B tries to update status with invalid fencing token - rejected!
        success_b = self.task_db.update_task_status(task_id, "worker-B", TASK_STATUS_INDEXED)
        self.assertFalse(success_b)

        # Worker A updates with valid fencing token - accepted!
        success_a = self.task_db.update_task_status(task_id, "worker-A", TASK_STATUS_INDEXED)
        self.assertTrue(success_a)

        updated_task = self.task_db.get_task(task_id)
        self.assertEqual(updated_task["status"], TASK_STATUS_INDEXED)
        self.assertIsNone(updated_task["lease_owner"])

    def test_consumer_commit_before_ack(self):
        """Consumer physically commits task in Task DB before issuing manual basic.ack."""
        consumer = TaskConsumer(self.task_db)
        channel = MockAMQPChannel()

        body = json.dumps({
            "event_id": "evt_msg_1",
            "file_id": 401,
            "file_name": "notes.txt",
            "sha256": "hash401",
        }).encode("utf-8")

        result = consumer.handle_message(body, delivery_tag=1, channel=channel)
        self.assertTrue(result)

        # 1. Task must exist in DB
        task = self.task_db.get_task_by_event_id("evt_msg_1")
        self.assertIsNotNone(task)
        self.assertEqual(task["status"], TASK_STATUS_PENDING)

        # 2. ACK must have been issued
        self.assertIn(1, channel.acked)
        self.assertEqual(len(channel.nacked), 0)

    @patch("time.sleep", return_value=None)
    def test_consumer_db_failure_controlled_backoff_and_no_ack(self, mock_sleep):
        """On persistent DB failure, basic.ack is NEVER called, controlled sleep is applied, and nack(requeue=True) is sent."""
        consumer = TaskConsumer(self.task_db, db_failure_backoff_seconds=2.0)
        channel = MockAMQPChannel()

        with patch.object(self.task_db, "record_task", side_effect=sqlite3.OperationalError("database is locked")):
            body = json.dumps({
                "event_id": "evt_fail_1",
                "file_id": 501,
                "sha256": "hash501",
            }).encode("utf-8")

            result = consumer.handle_message(body, delivery_tag=2, channel=channel)
            self.assertFalse(result)

            # basic.ack must NEVER be called
            self.assertNotIn(2, channel.acked)
            # Controlled backoff sleep must be executed to prevent tight loop
            mock_sleep.assert_called_once_with(2.0)
            # basic.nack must be called with requeue=True
            self.assertIn((2, True), channel.nacked)
            self.assertEqual(consumer.consecutive_db_failures, 1)

    @patch("time.sleep", return_value=None)
    def test_consumer_repeated_db_failures_exponential_backoff(self, mock_sleep):
        """Verify consecutive DB failures trigger increasing backoff and reset upon success."""
        consumer = TaskConsumer(self.task_db, db_failure_backoff_seconds=1.0, max_db_failure_backoff_seconds=10.0)
        channel = MockAMQPChannel()

        body = json.dumps({
            "event_id": "evt_fail_multi",
            "file_id": 601,
            "sha256": "hash601",
        }).encode("utf-8")

        # Failure 1: 1.0s
        with patch.object(self.task_db, "record_task", side_effect=sqlite3.OperationalError("disk I/O error")):
            consumer.handle_message(body, delivery_tag=10, channel=channel)
            mock_sleep.assert_called_with(1.0)
            self.assertEqual(consumer.consecutive_db_failures, 1)

        # Failure 2: 2.0s
        with patch.object(self.task_db, "record_task", side_effect=sqlite3.OperationalError("disk I/O error")):
            consumer.handle_message(body, delivery_tag=11, channel=channel)
            mock_sleep.assert_called_with(2.0)
            self.assertEqual(consumer.consecutive_db_failures, 2)

        # Success on subsequent message: resets consecutive_db_failures
        consumer.handle_message(body, delivery_tag=12, channel=channel)
        self.assertEqual(consumer.consecutive_db_failures, 0)
        self.assertIn(12, channel.acked)

    def test_consumer_streaming_proxy_and_adapter_pipeline_sets_index_verify_pending(self):
        """Adapter delivery sets status to INDEX_VERIFY_PENDING (not INDEXED) awaiting physical vector verification."""
        adapter = MockDestinationAdapter(status=DeliveryStatus.SUCCESS)
        consumer = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            proxy_base_url="http://127.0.0.1:8090",
            worker_id="worker-test-1",
        )

        event = {
            "event_id": "evt_full_flow",
            "file_id": 777,
            "file_name": "contract.txt",
            "sha256": "contracthash",
        }
        self.task_db.record_task(event)

        with patch.object(consumer, "fetch_file_content", return_value=b"Contract contents here"):
            processed = consumer.process_next_task()
            self.assertTrue(processed)

            # Contract was delivered to adapter
            self.assertEqual(len(adapter.contracts), 1)
            self.assertEqual(adapter.contracts[0].source_file_id, "777")
            self.assertEqual(adapter.contracts[0].file_payload, b"Contract contents here")

            # CRITICAL BOUNDARY: Task status is INDEX_VERIFY_PENDING, NOT INDEXED
            task = self.task_db.get_task_by_event_id("evt_full_flow")
            self.assertEqual(task["status"], TASK_STATUS_INDEX_VERIFY_PENDING)
            self.assertNotEqual(task["status"], TASK_STATUS_INDEXED)
            self.assertIsNone(task["indexed_at"], "indexed_at must remain None until physical verification")

    def test_race_condition_newer_version_aborts_older_worker_delivery(self):
        """If a newer version of the file arrives while an older worker is processing,
        the older worker MUST abort delivery to the destination adapter to prevent obsolete registration.
        """
        adapter = MockDestinationAdapter(status=DeliveryStatus.SUCCESS)
        consumer = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            proxy_base_url="http://127.0.0.1:8090",
            worker_id="worker-slow-1",
        )

        # 1. Version 1 of file 888 arrives
        event_v1 = {
            "event_id": "evt_v1_888",
            "file_id": 888,
            "file_name": "policy.txt",
            "sha256": "sha_v1",
        }
        _, id_v1 = self.task_db.record_task(event_v1)

        # 2. Worker claims version 1 (status -> PROCESSING, lease_owner -> worker-slow-1)
        task_v1 = self.task_db.claim_task("worker-slow-1")
        self.assertIsNotNone(task_v1)
        self.assertEqual(task_v1["status"], TASK_STATUS_PROCESSING)

        # 3. RACE: While worker-slow-1 is in-flight, Version 2 of file 888 arrives!
        event_v2 = {
            "event_id": "evt_v2_888",
            "file_id": 888,
            "file_name": "policy.txt",
            "sha256": "sha_v2_latest",
        }
        _, id_v2 = self.task_db.record_task(event_v2)

        # 4. Worker 1 resumes and attempts to finish processing task_v1
        with patch.object(consumer, "fetch_file_content", return_value=b"Old Version 1 Content"):
            processed = consumer.process_claimed_task(task_v1)
            self.assertFalse(processed)

            # Contract must NOT have been delivered to destination adapter
            self.assertEqual(len(adapter.contracts), 0, "Obsolete version must not be delivered to adapter!")

            # Task 1 must be SUPERSEDED
            task1_db = self.task_db.get_task(id_v1)
            self.assertEqual(task1_db["status"], TASK_STATUS_SUPERSEDED)

        # 5. Worker 2 claims Version 2 and succeeds
        consumer_fast = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            worker_id="worker-fast-2",
        )
        task_v2 = self.task_db.claim_task("worker-fast-2")
        self.assertIsNotNone(task_v2)
        with patch.object(consumer_fast, "fetch_file_content", return_value=b"New Version 2 Content"):
            processed_v2 = consumer_fast.process_claimed_task(task_v2)
            self.assertTrue(processed_v2)
            self.assertEqual(len(adapter.contracts), 1)
            self.assertEqual(adapter.contracts[0].file_payload, b"New Version 2 Content")

    def test_task_superseded_during_deliver_is_not_marked_current_or_verified(self):
        """If a newer version arrives DURING destination_adapter.deliver() execution,
        the older task MUST NOT be marked as current or advanced to INDEX_VERIFY_PENDING.
        """
        # Create an adapter that simulates a newer version arriving DURING deliver()
        class InFlightRaceAdapter(MockDestinationAdapter):
            def __init__(self, task_db_ref):
                super().__init__()
                self.task_db = task_db_ref

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                # SIMULATE RACE: Version 2 arrives in the middle of this HTTP deliver call!
                self.task_db.record_task({
                    "event_id": "evt_v2_999",
                    "file_id": 999,
                    "file_name": "memo.txt",
                    "sha256": "v2_hash_newest",
                })
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="HTTP 200 Ingestion OK", doc_location="arch_999_v1.txt")

        adapter = InFlightRaceAdapter(self.task_db)
        consumer = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            worker_id="worker-race-1",
        )

        # 1. Version 1 recorded and claimed
        _, id_v1 = self.task_db.record_task({
            "event_id": "evt_v1_999",
            "file_id": 999,
            "file_name": "memo.txt",
            "sha256": "v1_hash_old",
        })
        task_v1 = self.task_db.claim_task("worker-race-1")

        # 2. Process task 1 (the race triggers inside deliver)
        with patch.object(consumer, "fetch_file_content", return_value=b"Old Version 1 Content"):
            result = consumer.process_claimed_task(task_v1)
            # Worker must report failure/aborted state
            self.assertFalse(result)

        # 3. Verify Task 1 is SUPERSEDED, NOT INDEX_VERIFY_PENDING
        task1_db = self.task_db.get_task(id_v1)
        self.assertEqual(task1_db["status"], TASK_STATUS_SUPERSEDED)
        self.assertIsNone(task1_db["indexed_at"])

        # 4. Verify get_current_task_for_file returns Version 2, NEVER Version 1
        current_task = self.task_db.get_current_task_for_file(999)
        self.assertIsNotNone(current_task)
        self.assertEqual(current_task["event_id"], "evt_v2_999")
        self.assertEqual(current_task["sha256"], "v2_hash_newest")


    def test_inflight_superseded_version_removes_old_doc_and_preserves_new_doc(self):
        """Simulate a newer version arriving during deliver() of an older version.
        Confirms:
        1. Task 1 is marked SUPERSEDED and records its unique doc_location.
        2. Slower worker purges ONLY the old document from workspace via adapter.
        3. Task 2 processes successfully, records its own doc_location, and is NOT removed from workspace.
        """
        class InFlightRaceWithRemovalAdapter(MockDestinationAdapter):
            def __init__(self, task_db_ref):
                super().__init__()
                self.task_db = task_db_ref
                self.deliver_calls = []

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.deliver_calls.append(contract)
                if contract.content_hash == "hash_v1_old":
                    # SIMULATE IN-FLIGHT RACE: Version 2 arrives while v1 is delivering!
                    self.task_db.record_task({
                        "event_id": "evt_v2_1001",
                        "file_id": 1001,
                        "file_name": "annual_report.txt",
                        "sha256": "hash_v2_new",
                    })
                    return DeliveryResult(
                        status=DeliveryStatus.SUCCESS,
                        message="HTTP 200 OK",
                        doc_location="custom-documents/arch_1001_v1.json",
                    )
                else:
                    return DeliveryResult(
                        status=DeliveryStatus.SUCCESS,
                        message="HTTP 200 OK",
                        doc_location="custom-documents/arch_1001_v2.json",
                    )

            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                self.removed_documents.append(doc_location)
                return True

        adapter = InFlightRaceWithRemovalAdapter(self.task_db)
        consumer_v1 = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            worker_id="worker-old-1",
        )

        # 1. Version 1 recorded and claimed
        _, id_v1 = self.task_db.record_task({
            "event_id": "evt_v1_1001",
            "file_id": 1001,
            "file_name": "annual_report.txt",
            "sha256": "hash_v1_old",
        })
        task_v1 = self.task_db.claim_task("worker-old-1")
        self.assertIsNotNone(task_v1)

        # 2. Process Version 1 (triggers race condition inside deliver)
        with patch.object(consumer_v1, "fetch_file_content", return_value=b"Old V1 Content"):
            res_v1 = consumer_v1.process_claimed_task(task_v1)
            self.assertFalse(res_v1)

        # 3. Verify Version 1 state in Task DB: status=SUPERSEDED, doc_location recorded
        task_v1_db = self.task_db.get_task(id_v1)
        self.assertEqual(task_v1_db["status"], TASK_STATUS_SUPERSEDED)
        self.assertEqual(task_v1_db["doc_location"], "custom-documents/arch_1001_v1.json")

        # 4. CRITICAL: Verify adapter removed ONLY the old document from workspace!
        self.assertIn("custom-documents/arch_1001_v1.json", adapter.removed_documents)
        self.assertEqual(len(adapter.removed_documents), 1)

        # 5. Worker claims and processes Version 2
        consumer_v2 = TaskConsumer(
            self.task_db,
            destination_adapter=adapter,
            worker_id="worker-new-2",
        )
        task_v2 = self.task_db.claim_task("worker-new-2")
        self.assertIsNotNone(task_v2)
        self.assertEqual(task_v2["event_id"], "evt_v2_1001")

        with patch.object(consumer_v2, "fetch_file_content", return_value=b"New V2 Content"):
            res_v2 = consumer_v2.process_claimed_task(task_v2)
            self.assertTrue(res_v2)

        # 6. Verify Version 2 state in Task DB: status=INDEX_VERIFY_PENDING, doc_location recorded
        task_v2_db = self.task_db.get_task_by_event_id("evt_v2_1001")
        self.assertEqual(task_v2_db["status"], TASK_STATUS_INDEX_VERIFY_PENDING)
        self.assertEqual(task_v2_db["doc_location"], "custom-documents/arch_1001_v2.json")

        # 7. CRITICAL: Verify Version 2 was NOT removed from workspace!
        self.assertNotIn("custom-documents/arch_1001_v2.json", adapter.removed_documents)
        self.assertEqual(len(adapter.removed_documents), 1)

    def test_superseded_cleanup_failure_records_cleanup_pending_in_task_db(self):
        """When removal of an obsolete document fails after in-flight deliver(), Task DB must record
        cleanup_pending=1 and preserve the error instead of pretending cleanup completed.
        """
        class InFlightFailingRemovalAdapter(MockDestinationAdapter):
            def __init__(self, task_db_ref):
                super().__init__()
                self.task_db = task_db_ref

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                # SIMULATE IN-FLIGHT RACE: Version 2 arrives during deliver()
                self.task_db.record_task({
                    "event_id": "evt_v2_fail",
                    "file_id": 2001,
                    "file_name": "data.txt",
                    "sha256": "hash_v2",
                })
                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    message="OK",
                    doc_location="custom-documents/failing_old.json"
                )

            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                # Return failure (e.g. workspace lock or invalid response)
                return False

        adapter = InFlightFailingRemovalAdapter(self.task_db)
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-failing-1")

        # 1. Version 1 arrives and claimed
        _, id_v1 = self.task_db.record_task({
            "event_id": "evt_v1_fail",
            "file_id": 2001,
            "file_name": "data.txt",
            "sha256": "hash_v1",
        })
        task_v1 = self.task_db.claim_task("worker-failing-1")

        # 2. Worker 1 processes task 1: race happens in deliver(), removal fails
        with patch.object(consumer, "fetch_file_content", return_value=b"V1 bytes"):
            res = consumer.process_claimed_task(task_v1)
            self.assertFalse(res)

        # 4. Verify Task DB reflects failure: cleanup_pending must be 1, last_error populated!
        t1 = self.task_db.get_task(id_v1)
        self.assertEqual(t1["status"], TASK_STATUS_SUPERSEDED)
        self.assertEqual(t1["doc_location"], "custom-documents/failing_old.json")
        self.assertEqual(t1["cleanup_pending"], 1)
        self.assertIsNotNone(t1["last_error"])
        self.assertIn("rejected", t1["last_error"])

    def test_retry_superseded_cleanups_succeeds_and_clears_pending(self):
        """Verifies retry_superseded_cleanups targets strictly the pending superseded document
        and clears cleanup_pending upon successful removal.
        """
        removed_locations = []
        cleanup_should_succeed = False

        class ToggleRemovalAdapter:
            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                removed_locations.append(doc_location)
                return cleanup_should_succeed

        adapter = ToggleRemovalAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-retry-1")

        # Create a task marked with cleanup_pending=1
        _, task_id = self.task_db.record_task({
            "event_id": "evt_retry_test",
            "file_id": 3001,
            "file_name": "memo.txt",
            "sha256": "hash_retry",
        })
        self.task_db.set_task_doc_location(task_id, "custom-documents/retry_doc.json", workspace_slug="ai-box-test")
        self.task_db.mark_task_cleanup_pending(task_id, error="Initial transient failure", retry_delay_seconds=-1)

        # Confirm task is pending cleanup
        pending = self.task_db.get_pending_cleanup_tasks()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["id"], task_id)

        # First retry attempt fails
        cleanup_should_succeed = False
        successes = consumer.retry_superseded_cleanups()
        self.assertEqual(successes, 0)
        t_after_fail = self.task_db.get_task(task_id)
        self.assertEqual(t_after_fail["cleanup_pending"], 1)
        self.assertEqual(len(removed_locations), 1)
        self.assertEqual(removed_locations[0], "custom-documents/retry_doc.json")

        # Second retry attempt succeeds
        cleanup_should_succeed = True
        # Set next_retry_at to past so it qualifies immediately
        self.task_db.mark_task_cleanup_pending(task_id, error="Transient", retry_delay_seconds=-1)
        successes = consumer.retry_superseded_cleanups()
        self.assertEqual(successes, 1)

        # Task DB state must now be fully cleared
        t_after_success = self.task_db.get_task(task_id)
        self.assertEqual(t_after_success["cleanup_pending"], 0)
        self.assertIsNone(t_after_success["last_error"])
        self.assertEqual(len(self.task_db.get_pending_cleanup_tasks()), 0)

    def test_retry_superseded_cleanups_exhausts_retries_and_marks_needs_review(self):
        """When cleanup retries exceed max_retries, task transitions to NEEDS_REVIEW."""
        class AlwaysFailingAdapter:
            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                return False

        adapter = AlwaysFailingAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-exhaust-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_exhaust_test",
            "file_id": 4001,
            "file_name": "fail.txt",
            "sha256": "hash_exhaust",
        })
        self.task_db.set_task_doc_location(task_id, "custom-documents/exhaust.json")

        # Exhaust retries
        for i in range(10):
            self.task_db.mark_task_cleanup_pending(task_id, error="Persistent failure", retry_delay_seconds=-1)

        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertEqual(t["cleanup_pending"], 1)

    def test_superseded_cleanup_targets_specific_workspace_slug_not_default(self):
        """1. Verifies old document cleanup strictly targets the recorded workspace_slug, NOT the default."""
        targeted_calls = []

        class SpecificWorkspaceAdapter(MockDestinationAdapter):
            def __init__(self, task_db_ref):
                super().__init__()
                self.task_db = task_db_ref
                self.workspace_slug = "default-public-ws"

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                return "finance-restricted-ws"

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                # SIMULATE IN-FLIGHT RACE: Version 2 arrives during deliver()
                self.task_db.record_task({
                    "event_id": "evt_v2_ws",
                    "file_id": 5001,
                    "file_name": "budget.txt",
                    "sha256": "hash_v2",
                })
                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    message="OK",
                    doc_location="custom-documents/specific_doc.json",
                    workspace_slug="finance-restricted-ws",
                )

            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                targeted_calls.append((doc_location, workspace_slug))
                return True

        adapter = SpecificWorkspaceAdapter(self.task_db)
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-ws-1")

        # Record task 1 and claim
        _, id_v1 = self.task_db.record_task({
            "event_id": "evt_v1_ws",
            "file_id": 5001,
            "file_name": "budget.txt",
            "sha256": "hash_v1",
        })
        task_v1 = self.task_db.claim_task("worker-ws-1")

        # Process task 1 (the race triggers inside deliver)
        with patch.object(consumer, "fetch_file_content", return_value=b"Budget data"):
            consumer.process_claimed_task(task_v1)

        # Verify initial cleanup targeted the specific workspace, NOT default
        self.assertEqual(len(targeted_calls), 1)
        self.assertEqual(targeted_calls[0], ("custom-documents/specific_doc.json", "finance-restricted-ws"))

        # Verify workspace_slug is stored in Task DB
        t1 = self.task_db.get_task(id_v1)
        self.assertEqual(t1["workspace_slug"], "finance-restricted-ws")

        # Now test Retry targeting
        self.task_db.mark_task_cleanup_pending(id_v1, error="Retry test", retry_delay_seconds=-1)
        targeted_calls.clear()

        consumer.retry_superseded_cleanups()
        self.assertEqual(len(targeted_calls), 1)
        self.assertEqual(targeted_calls[0], ("custom-documents/specific_doc.json", "finance-restricted-ws"))

    def test_cleanup_retry_does_not_affect_main_processing_retry_count(self):
        """2. Verifies cleanup retry increments cleanup_retry_count independently and leaves main retry_count untouched."""
        class DummyFailingAdapter:
            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                return False

        adapter = DummyFailingAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-indep-1")

        # Create task with existing main processing retry_count = 3
        _, task_id = self.task_db.record_task({
            "event_id": "evt_indep_test",
            "file_id": 6001,
            "file_name": "memo.txt",
            "sha256": "hash_indep",
        })
        with self.task_db._get_connection() as conn:
            conn.execute("UPDATE ai_box_tasks SET retry_count = 3, doc_location = 'doc.json', workspace_slug = 'ai-box-test' WHERE id = ?", (task_id,))
            conn.commit()

        # Simulate 2 failed cleanup retries
        self.task_db.mark_task_cleanup_pending(task_id, error="fail 1", retry_delay_seconds=-1)
        consumer.retry_superseded_cleanups()

        t_after = self.task_db.get_task(task_id)
        # Main processing retry_count MUST remain exactly 3!
        self.assertEqual(t_after["retry_count"], 3)
        # Independent cleanup_retry_count must be incremented to 2
        self.assertEqual(t_after["cleanup_retry_count"], 2)

    def test_needs_review_record_halted_from_automatic_retry_cycle(self):
        """3. Verifies NEEDS_REVIEW records are strictly excluded from automatic retry cycle while preserving error details."""
        cleanup_attempts = []

        class TrackingAdapter:
            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                cleanup_attempts.append(doc_location)
                return False

        adapter = TrackingAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-halt-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_halt_test",
            "file_id": 7001,
            "file_name": "data.txt",
            "sha256": "hash_halt",
        })
        self.task_db.set_task_doc_location(task_id, "custom-documents/halt_doc.json", workspace_slug="ai-box-test")

        # Exhaust cleanup retries (max_cleanup_retries = 5)
        for i in range(5):
            self.task_db.mark_task_cleanup_pending(task_id, error=f"Failure #{i+1}", retry_delay_seconds=-1, max_cleanup_retries=5)

        t_exhausted = self.task_db.get_task(task_id)
        self.assertEqual(t_exhausted["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertEqual(t_exhausted["cleanup_pending"], 1)
        self.assertEqual(t_exhausted["last_error"], "Failure #5")
        self.assertIsNone(t_exhausted["next_retry_at"])

        # Run automatic retry cycle
        cleanup_attempts.clear()
        retried_count = consumer.retry_superseded_cleanups()
        self.assertEqual(retried_count, 0)
        self.assertEqual(len(cleanup_attempts), 0, "NEEDS_REVIEW records must NEVER be picked up by automatic retry!")

    def test_cleanup_failure_does_not_declare_false_success(self):
        """4. Verifies cleanup failure does not declare false success and leaves cleanup_pending=1."""
        class RejectionAdapter:
            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                return False

        adapter = RejectionAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-false-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_false_succ",
            "file_id": 8001,
            "file_name": "policy.txt",
            "sha256": "hash_false",
        })
        self.task_db.set_task_doc_location(task_id, "custom-documents/policy.json", workspace_slug="ai-box-test")

        # Mark pending and run retry
        self.task_db.mark_task_cleanup_pending(task_id, error="Transient fail", retry_delay_seconds=-1)
        successes = consumer.retry_superseded_cleanups()

        self.assertEqual(successes, 0)
        t = self.task_db.get_task(task_id)
        self.assertEqual(t["cleanup_pending"], 1)
        self.assertIsNotNone(t["last_error"])
        self.assertIn("Adapter rejected removal", t["last_error"])


    def test_valid_group_routes_to_group_workspace(self):
        """1. Verifies a valid single group routes to its specific group workspace slug."""
        class GroupResolvingAdapter:
            def __init__(self):
                self.contracts = []

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                return f"{group_name.lower()}-workspace"

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.contracts.append(contract)
                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    message="Ingested",
                    doc_location=f"custom-documents/arch_{contract.source_file_id}.json",
                    workspace_slug=contract.workspace_slug,
                )

        adapter = GroupResolvingAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-grp-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_legal_01",
            "file_id": 9101,
            "file_name": "contract.pdf",
            "group_name": "Legal",
            "sha256": "legalhash999",
        })
        claimed = self.task_db.claim_task("worker-grp-1")

        with patch.object(consumer, "fetch_file_content", return_value=b"NDA Contract"):
            ok = consumer.process_claimed_task(claimed)

        self.assertTrue(ok)
        self.assertEqual(len(adapter.contracts), 1)
        self.assertEqual(adapter.contracts[0].workspace_slug, "legal-workspace")

        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_INDEX_VERIFY_PENDING)
        self.assertEqual(t["workspace_slug"], "legal-workspace")

    def test_ambiguous_and_unspecified_group_routes_to_needs_review(self):
        """2. Verifies unspecified, multi-group, or ambiguous groups transition to NEEDS_REVIEW without calling deliver."""
        adapter = MockDestinationAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-amb-1")

        # 2a. Multi-group assignment
        _, id_multi = self.task_db.record_task({
            "event_id": "evt_multi_01",
            "file_id": 9201,
            "file_name": "memo.txt",
            "group_name": "Finance, Legal",
            "sha256": "multihash",
        })
        claimed_multi = self.task_db.claim_task("worker-amb-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"memo"):
            ok_multi = consumer.process_claimed_task(claimed_multi)

        self.assertFalse(ok_multi)
        t_multi = self.task_db.get_task(id_multi)
        self.assertEqual(t_multi["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("ambiguous", t_multi["last_error"])
        self.assertEqual(len(adapter.contracts), 0, "deliver() must NOT be called for multi-group tasks")

        # 2b. Unspecified / empty group
        _, id_empty = self.task_db.record_task({
            "event_id": "evt_empty_01",
            "file_id": 9202,
            "file_name": "memo2.txt",
            "group_name": "   ",
            "sha256": "emptyhash",
        })
        claimed_empty = self.task_db.claim_task("worker-amb-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"memo2"):
            ok_empty = consumer.process_claimed_task(claimed_empty)

        self.assertFalse(ok_empty)
        t_empty = self.task_db.get_task(id_empty)
        self.assertEqual(t_empty["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("empty or unspecified", t_empty["last_error"])

        # 2c. Invalid / unknown group
        _, id_unknown = self.task_db.record_task({
            "event_id": "evt_unknown_01",
            "file_id": 9203,
            "file_name": "memo3.txt",
            "group_name": "unknown",
            "sha256": "unknownhash",
        })
        claimed_unknown = self.task_db.claim_task("worker-amb-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"memo3"):
            ok_unknown = consumer.process_claimed_task(claimed_unknown)

        self.assertFalse(ok_unknown)
        t_unknown = self.task_db.get_task(id_unknown)
        self.assertEqual(t_unknown["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("invalid/unknown", t_unknown["last_error"])

    def test_cleanup_prevents_deletion_in_default_workspace_when_workspace_unknown(self):
        """3. Verifies cleanup refrains from deleting in default workspace when original workspace is unknown."""
        removed_calls = []

        class GuardedRemovalAdapter:
            def __init__(self):
                self.workspace_slug = "ai-box-default-workspace"

            def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
                removed_calls.append((doc_location, workspace_slug))
                return True

        adapter = GuardedRemovalAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-guard-1")

        # 3a. Retry cleanup when workspace_slug is None
        _, id_unk = self.task_db.record_task({
            "event_id": "evt_unk_ws",
            "file_id": 9301,
            "file_name": "orphan.txt",
            "sha256": "orphan_hash",
        })
        # Set doc_location but explicitly leave workspace_slug as None
        self.task_db.set_task_doc_location(id_unk, "custom-documents/orphan.json", workspace_slug=None)
        self.task_db.mark_task_cleanup_pending(id_unk, error="Prior error", retry_delay_seconds=-1)

        successes = consumer.retry_superseded_cleanups()
        self.assertEqual(successes, 0)
        self.assertEqual(len(removed_calls), 0, "Must NEVER invoke document removal in default workspace if workspace is unknown!")

        t_after = self.task_db.get_task(id_unk)
        self.assertEqual(t_after["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertEqual(t_after["cleanup_pending"], 1)
        self.assertIn("Missing workspace_slug", t_after["last_error"])


    def test_absence_of_resolver_rejects_default_workspace_and_transitions_to_needs_review(self):
        """1. Verifies that absence of resolver on destination adapter halts routing and transitions to NEEDS_REVIEW."""
        class AdapterWithoutResolver:
            def __init__(self):
                self.workspace_slug = "default-public-ws"
                self.deliver_called = False

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.deliver_called = True
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = AdapterWithoutResolver()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-no-res-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_no_res_01",
            "file_id": 9501,
            "file_name": "memo.txt",
            "group_name": "Finance",
            "sha256": "sha_no_res_01",
        })
        claimed = self.task_db.claim_task("worker-no-res-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"content"):
            processed = consumer.process_claimed_task(claimed)

        self.assertFalse(processed, "Processing must fail when adapter lacks an official resolver.")
        self.assertFalse(adapter.deliver_called, "deliver() must NEVER be called without an official resolver!")

        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("does not provide an official group workspace resolver", t["last_error"])
        self.assertIn("forbidden", t["last_error"])

    def test_guessed_slug_is_rejected_without_official_resolver(self):
        """2. Verifies that guessed or fabricated workspace slugs are strictly rejected."""
        class RejectingResolverAdapter:
            def __init__(self):
                self.deliver_called = False

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                # Return empty/None to simulate resolver inability to map
                return ""

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.deliver_called = True
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = RejectingResolverAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-no-guess-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_no_guess_01",
            "file_id": 9502,
            "file_name": "memo2.txt",
            "group_name": "Finance",
            "sha256": "sha_no_guess_01",
        })
        claimed = self.task_db.claim_task("worker-no-guess-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"content"):
            processed = consumer.process_claimed_task(claimed)

        self.assertFalse(processed)
        self.assertFalse(adapter.deliver_called)

        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("returned empty slug", t["last_error"])

    def test_only_valid_group_from_official_archive_is_mapped(self):
        """3. Verifies that only a valid group extracted from official Archive source is mapped and delivered."""
        class OfficialArchiveGroupAdapter:
            def __init__(self):
                self.delivered_contracts = []

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                if group_name == "Human Resources":
                    return "hr-official-workspace"
                raise ValueError(f"Unrecognized group: {group_name}")

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.delivered_contracts.append(contract)
                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    message="Ingested",
                    doc_location=f"custom-documents/arch_{contract.source_file_id}.json",
                    workspace_slug=contract.workspace_slug,
                )

        adapter = OfficialArchiveGroupAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-official-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_hr_01",
            "file_id": 9503,
            "file_name": "policy.pdf",
            "group_name": "Human Resources",
            "sha256": "sha_hr_01",
        })
        claimed = self.task_db.claim_task("worker-official-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"HR Policy Content"):
            processed = consumer.process_claimed_task(claimed)

        self.assertTrue(processed)
        self.assertEqual(len(adapter.delivered_contracts), 1)
        self.assertEqual(adapter.delivered_contracts[0].workspace_slug, "hr-official-workspace")

        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_INDEX_VERIFY_PENDING)
        self.assertEqual(t["workspace_slug"], "hr-official-workspace")

    def test_group_ambiguity_prevents_file_delivery(self):
        """4. Verifies that multi-group, unknown group, and AmbiguousWorkspaceError prevent sending to destination."""
        class AmbiguityDetectingAdapter:
            def __init__(self):
                self.deliver_called = False

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                if group_name == "AmbiguousDept":
                    from destination_adapter import AmbiguousWorkspaceError
                    raise AmbiguousWorkspaceError("Multiple workspaces match 'AmbiguousDept'")
                return f"{group_name.lower()}-ws"

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.deliver_called = True
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = AmbiguityDetectingAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-ambig-1")

        # 4a. Multi-group (Finance, Legal)
        _, id_multi = self.task_db.record_task({
            "event_id": "evt_ambig_multi",
            "file_id": 9504,
            "file_name": "joint_review.txt",
            "group_name": "Finance, Legal",
            "sha256": "sha_multi_ambig",
        })
        claimed_multi = self.task_db.claim_task("worker-ambig-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"data"):
            ok_multi = consumer.process_claimed_task(claimed_multi)
        self.assertFalse(ok_multi)
        self.assertFalse(adapter.deliver_called)
        self.assertEqual(self.task_db.get_task(id_multi)["status"], TASK_STATUS_NEEDS_REVIEW)

        # 4b. UNKNOWN group from Archive (zero groups resolved)
        _, id_unk = self.task_db.record_task({
            "event_id": "evt_ambig_unk",
            "file_id": 9505,
            "file_name": "orphan.txt",
            "group_name": "UNKNOWN",
            "sha256": "sha_unk_ambig",
        })
        claimed_unk = self.task_db.claim_task("worker-ambig-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"data"):
            ok_unk = consumer.process_claimed_task(claimed_unk)
        self.assertFalse(ok_unk)
        self.assertFalse(adapter.deliver_called)
        self.assertEqual(self.task_db.get_task(id_unk)["status"], TASK_STATUS_NEEDS_REVIEW)

        # 4c. Ambiguous workspace match from adapter
        _, id_match = self.task_db.record_task({
            "event_id": "evt_ambig_match",
            "file_id": 9506,
            "file_name": "dept.txt",
            "group_name": "AmbiguousDept",
            "sha256": "sha_match_ambig",
        })
        claimed_match = self.task_db.claim_task("worker-ambig-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"data"):
            ok_match = consumer.process_claimed_task(claimed_match)
        self.assertFalse(ok_match)
        self.assertFalse(adapter.deliver_called)
        self.assertEqual(self.task_db.get_task(id_match)["status"], TASK_STATUS_NEEDS_REVIEW)

    def test_transient_network_error_retries_while_resolution_rejection_needs_review(self):
        """5. Verifies transient connection error sets RETRY_WAIT while non-transient rejection sets NEEDS_REVIEW."""
        class TransientErrorAdapter:
            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                import urllib.error
                raise urllib.error.URLError("Connection refused: AnythingLLM gateway unreachable")

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = TransientErrorAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-trans-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_transient_01",
            "file_id": 9507,
            "file_name": "network.txt",
            "group_name": "Finance",
            "sha256": "sha_trans_01",
        })
        claimed = self.task_db.claim_task("worker-trans-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"data"):
            ok = consumer.process_claimed_task(claimed)

        self.assertFalse(ok)
        t = self.task_db.get_task(task_id)
        # MUST be RETRY_WAIT because network connection error is genuinely transient!
        self.assertEqual(t["status"], TASK_STATUS_RETRY_WAIT)
        self.assertEqual(t["retry_count"], 1)
        self.assertIsNotNone(t["next_retry_at"])


    def test_permanent_http_errors_reach_needs_review_and_do_not_retry(self):
        """Verifies HTTP 401, 403, and 400 errors during workspace resolution transition to NEEDS_REVIEW."""
        for code, err_name in [(401, "Unauthorized"), (403, "Forbidden"), (400, "Bad Request")]:
            class PermanentHttpErrorAdapter:
                def __init__(self):
                    self.deliver_called = False

                def resolve_or_create_group_workspace(self, group_name: str) -> str:
                    import urllib.error
                    fp = MagicMock()
                    fp.read.return_value = b'{"error": "Access Denied"}'
                    raise urllib.error.HTTPError(
                        url="http://127.0.0.1:3001/api/v1/workspaces",
                        code=code,
                        msg=err_name,
                        hdrs={},
                        fp=fp,
                    )

                def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                    self.deliver_called = True
                    return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

            adapter = PermanentHttpErrorAdapter()
            consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id=f"worker-perm-{code}")

            _, task_id = self.task_db.record_task({
                "event_id": f"evt_perm_{code}",
                "file_id": 9600 + code,
                "file_name": f"policy_{code}.txt",
                "group_name": "Finance",
                "sha256": f"sha_perm_{code}",
            })
            claimed = self.task_db.claim_task(f"worker-perm-{code}")
            with patch.object(consumer, "fetch_file_content", return_value=b"data"):
                ok = consumer.process_claimed_task(claimed)

            self.assertFalse(ok, f"Processing must fail for HTTP {code}")
            self.assertFalse(adapter.deliver_called, f"deliver() must NEVER be called on HTTP {code}")
            t = self.task_db.get_task(task_id)
            self.assertEqual(t["status"], TASK_STATUS_NEEDS_REVIEW, f"HTTP {code} must transition to NEEDS_REVIEW")
            self.assertEqual(t["retry_count"], 0, f"HTTP {code} is permanent and must not increment retry_count")

    def test_http_error_subclass_of_urlerror_not_misclassified_as_transient(self):
        """Verifies that HTTPError (which inherits from URLError) is NOT misclassified as a transient network error."""
        import urllib.error
        fp = MagicMock()
        fp.read.return_value = b'{"error": "Client Error"}'
        http_404 = urllib.error.HTTPError(
            url="http://127.0.0.1:3001/api/v1/workspaces",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=fp,
        )
        # Verify precondition that HTTPError is an instance of URLError in Python
        self.assertIsInstance(http_404, urllib.error.URLError)

        class Http404Adapter:
            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                raise http_404

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = Http404Adapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-404-1")

        slug, err, is_transient = consumer.resolve_workspace_for_task({"group_name": "Finance"})
        self.assertIsNone(slug)
        self.assertFalse(is_transient, "HTTPError 404 must NOT be classified as transient even though it inherits from URLError!")
        self.assertIn("404", err)

    def test_transient_http_5xx_server_error_reaches_retry_wait(self):
        """Verifies HTTP 500/503 during workspace resolution transition to RETRY_WAIT."""
        class Server503Adapter:
            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                import urllib.error
                fp = MagicMock()
                fp.read.return_value = b'{"error": "Service Unavailable"}'
                raise urllib.error.HTTPError(
                    url="http://127.0.0.1:3001/api/v1/workspaces",
                    code=503,
                    msg="Service Unavailable",
                    hdrs={},
                    fp=fp,
                )

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = Server503Adapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-503-1")

        _, task_id = self.task_db.record_task({
            "event_id": "evt_503_01",
            "file_id": 9508,
            "file_name": "server_err.txt",
            "group_name": "Finance",
            "sha256": "sha_503_01",
        })
        claimed = self.task_db.claim_task("worker-503-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"data"):
            ok = consumer.process_claimed_task(claimed)

        self.assertFalse(ok)
        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_RETRY_WAIT, "HTTP 503 must transition to RETRY_WAIT")
        self.assertEqual(t["retry_count"], 1)
        self.assertIsNotNone(t["next_retry_at"])

    def test_group_grant_or_uploader_membership_not_in_task_group_name_reaches_needs_review(self):
        """Verifies that an event with UNKNOWN group (from unauthoritative file) never delivers and reaches NEEDS_REVIEW."""
        class StrictAdapter:
            def __init__(self):
                self.deliver_called = False

            def resolve_or_create_group_workspace(self, group_name: str) -> str:
                return "default-workspace"

            def deliver(self, contract: DeliveryContract) -> DeliveryResult:
                self.deliver_called = True
                return DeliveryResult(status=DeliveryStatus.SUCCESS, message="OK")

        adapter = StrictAdapter()
        consumer = TaskConsumer(self.task_db, destination_adapter=adapter, worker_id="worker-unk-review-1")

        # Simulate task emitted by Archive Outbox with UNKNOWN group
        _, task_id = self.task_db.record_task({
            "event_id": "evt_unk_override_01",
            "file_id": 9509,
            "file_name": "personal_grant_file.txt",
            "group_name": "UNKNOWN",
            "sha256": "sha_unk_override_01",
        })
        claimed = self.task_db.claim_task("worker-unk-review-1")
        with patch.object(consumer, "fetch_file_content", return_value=b"confidential content"):
            ok = consumer.process_claimed_task(claimed)

        self.assertFalse(ok)
        self.assertFalse(adapter.deliver_called, "File with UNKNOWN group must NEVER call deliver()")
        t = self.task_db.get_task(task_id)
        self.assertEqual(t["status"], TASK_STATUS_NEEDS_REVIEW)
        self.assertIn("invalid/unknown", t["last_error"])

if __name__ == "__main__":
    unittest.main()
