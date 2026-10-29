# -*- coding: utf-8 -*-
import unittest
import os
import sys
import tempfile
import shutil
from unittest.mock import MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import ConnectorConfig
from state_store import StateStore
from core import CoreTransferEngine
from destination_adapter import DeliveryStatus, DeliveryResult
from source_webdav import WebDAVFileMetadata

class TestCoreIntegration(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.test_dir, "sync_state.json")
        self.state_store = StateStore(self.state_file)
        self.config = ConnectorConfig(
            webdav_base_url="http://127.0.0.1:80",
            webdav_user="mock_user",
            webdav_password="mock_password",
            webdav_folder="ai_inbox",
            destination_gateway_url="http://127.0.0.1:3001",
            destination_workspace_slug="ai-box-test"
        )
        self.mock_adapter = MagicMock()
        self.mock_source = MagicMock()
        self.engine = CoreTransferEngine(self.config, self.state_store, self.mock_adapter, source_client=self.mock_source)

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_normal_sync_flow(self):
        self.mock_adapter.deliver.return_value = DeliveryResult(
            status=DeliveryStatus.SUCCESS,
            message="Indexed in LanceDB",
            doc_location="custom-documents/doc.json",
            target_filename="arch_100_abc_test.txt"
        )
        res = self.engine.process_file_payload("100", "test.txt", b"valid content")
        self.assertEqual(res["status"], DeliveryStatus.SUCCESS.value)

        rec = self.state_store.get("100")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.status, DeliveryStatus.SUCCESS.value)

    def test_version_conflict_marks_needs_review(self):
        # 1. First sync success
        self.mock_adapter.deliver.return_value = DeliveryResult(
            status=DeliveryStatus.SUCCESS,
            message="Initial sync",
            target_filename="arch_100_v1.txt"
        )
        self.engine.process_file_payload("100", "test.txt", b"content v1")

        # 2. Same file ID arrives with changed content -> Must halt and mark NEEDS_REVIEW
        res = self.engine.process_file_payload("100", "test.txt", b"content v2 altered")
        self.assertEqual(res["status"], DeliveryStatus.NEEDS_REVIEW.value)
        self.assertIn("Auto-resend halted", res["message"])

        rec = self.state_store.get("100")
        self.assertEqual(rec.status, DeliveryStatus.NEEDS_REVIEW.value)

    def test_needs_review_prevents_auto_resend(self):
        self.state_store.update_record("200", "conflict.txt", "hash", "NEEDS_REVIEW")
        res = self.engine.process_file_payload("200", "conflict.txt", b"any content")
        self.assertEqual(res["status"], DeliveryStatus.NEEDS_REVIEW.value)
        self.mock_adapter.deliver.assert_not_called()

    def test_sync_folder_loop(self):
        self.mock_source.list_folder.return_value = [
            WebDAVFileMetadata(file_id="301", href="/inbox/f1.txt", name="f1.txt", size=10, last_modified=""),
            WebDAVFileMetadata(file_id="302", href="/inbox/f2.txt", name="f2.txt", size=20, last_modified="")
        ]
        self.mock_source.download_file.return_value = b"sample content"
        self.mock_adapter.deliver.return_value = DeliveryResult(
            status=DeliveryStatus.SUCCESS,
            message="Success",
            target_filename="arch_file.txt"
        )

        stats = self.engine.sync_folder()
        self.assertEqual(stats["discovered"], 2)
        self.assertEqual(stats["synced"], 2)
        self.assertEqual(stats["errors"], 0)

if __name__ == "__main__":
    unittest.main()
