# -*- coding: utf-8 -*-
import unittest
import os
import sys
import tempfile
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from core import ConnectorLock, ConcurrencyError

class TestConnectorConcurrency(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.lock_path = os.path.join(self.test_dir, "test.lock")

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_lock_acquire_and_release(self):
        lock = ConnectorLock(self.lock_path)
        with lock:
            self.assertTrue(os.path.exists(self.lock_path))

    def test_overlapping_run_blocked(self):
        lock1 = ConnectorLock(self.lock_path)
        lock2 = ConnectorLock(self.lock_path)

        with lock1:
            # Second instance must raise ConcurrencyError
            with self.assertRaises(ConcurrencyError):
                lock2.acquire()

        # After lock1 releases, lock2 can acquire
        with lock2:
            self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()
