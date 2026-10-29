# -*- coding: utf-8 -*-
import subprocess
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

class TestMainCLI(unittest.TestCase):
    def setUp(self):
        self.connector_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        self.main_py = os.path.join(self.connector_dir, "main.py")

    def test_cli_help_exits_cleanly_without_network(self):
        res = subprocess.run(
            [sys.executable, self.main_py, "--help"],
            capture_output=True,
            text=True
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn("--check-config", res.stdout)
        self.assertIn("--run-once", res.stdout)

    def test_cli_check_config_redacts_password(self):
        env = dict(os.environ)
        env["WEBDAV_BASE_URL"] = "http://127.0.0.1:80/dav"
        env["WEBDAV_USER"] = "my_user"
        env["WEBDAV_PASSWORD"] = "SecretPasswordDoNotPrint"
        env["WEBDAV_FOLDER"] = "my_folder"

        res = subprocess.run(
            [sys.executable, self.main_py, "--check-config"],
            capture_output=True,
            text=True,
            env=env
        )
        self.assertEqual(res.returncode, 0)
        self.assertIn("[CONFIG OK]", res.stdout)
        self.assertIn("my_user", res.stdout)
        self.assertIn("[REDACTED]", res.stdout)
        self.assertNotIn("SecretPasswordDoNotPrint", res.stdout)

if __name__ == "__main__":
    unittest.main()
