# -*- coding: utf-8 -*-
import unittest
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from state_store import StateStore, StateCorruptionError

class TestStateStore(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.state_file = os.path.join(self.test_dir, "test_state.json")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_initial_state_empty(self):
        store = StateStore(self.state_file)
        self.assertEqual(len(store.all_records()), 0)
        self.assertIsNone(store.get("660"))

    def test_atomic_save_and_reload(self):
        store = StateStore(self.state_file)
        store.update_record(
            source_file_id="660",
            filename="doc.txt",
            content_hash="abc123hash",
            status="SUCCESS",
            target_filename="arch_660_abc123_doc.txt",
            doc_location="custom-documents/arch_660_abc123_doc.txt-uuid.json"
        )

        store2 = StateStore(self.state_file)
        rec = store2.get("660")
        self.assertIsNotNone(rec)
        self.assertEqual(rec.status, "SUCCESS")

    def test_corrupt_state_raises_and_backs_up(self):
        with open(self.state_file, "w", encoding="utf-8") as f:
            f.write("INVALID JSON CONTENT { [")

        with self.assertRaises(StateCorruptionError):
            StateStore(self.state_file)

        # Check backup file exists
        files = os.listdir(self.test_dir)
        corrupt_files = [fn for fn in files if "corrupt" in fn]
        self.assertTrue(len(corrupt_files) > 0)

    def test_supports_needs_review_status(self):
        store = StateStore(self.state_file)
        store.update_record("700", "conflict.txt", "hash", "NEEDS_REVIEW", error_message="Version conflict")
        rec = store.get("700")
        self.assertEqual(rec.status, "NEEDS_REVIEW")

    def test_relative_path_independent_of_cwd(self):
        orig_cwd = os.getcwd()
        try:
            # Change CWD to a different directory
            os.chdir(self.test_dir)
            store = StateStore("relative_state.json")
            # Must point inside connector directory, not inside self.test_dir
            connector_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
            self.assertTrue(store.filepath.startswith(connector_dir))
        finally:
            os.chdir(orig_cwd)

if __name__ == "__main__":
    unittest.main()
