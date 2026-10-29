# -*- coding: utf-8 -*-
import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config import load_config, ConfigError

class TestConnectorConfig(unittest.TestCase):
    def test_missing_webdav_url_raises(self):
        with self.assertRaises(ConfigError):
            load_config(env_dict={"WEBDAV_USER": "u", "WEBDAV_PASSWORD": "p", "WEBDAV_FOLDER": "f"})

    def test_missing_webdav_user_raises(self):
        with self.assertRaises(ConfigError):
            load_config(env_dict={"WEBDAV_BASE_URL": "http://127.0.0.1:80", "WEBDAV_PASSWORD": "p", "WEBDAV_FOLDER": "f"})

    def test_missing_webdav_password_raises(self):
        with self.assertRaises(ConfigError):
            load_config(env_dict={"WEBDAV_BASE_URL": "http://127.0.0.1:80", "WEBDAV_USER": "u", "WEBDAV_FOLDER": "f"})

    def test_missing_webdav_folder_raises(self):
        with self.assertRaises(ConfigError):
            load_config(env_dict={"WEBDAV_BASE_URL": "http://127.0.0.1:80", "WEBDAV_USER": "u", "WEBDAV_PASSWORD": "p", "WEBDAV_FOLDER": ""})

    def test_password_hidden_in_repr(self):
        cfg = load_config(env_dict={
            "WEBDAV_BASE_URL": "http://127.0.0.1:80/dav",
            "WEBDAV_USER": "test_user",
            "WEBDAV_PASSWORD": "SuperSecretPassword123!",
            "WEBDAV_FOLDER": "ai_inbox",
        })
        rep = repr(cfg)
        self.assertNotIn("SuperSecretPassword123!", rep)
        self.assertNotIn("webdav_password", rep)

    def test_insecure_remote_http_rejected(self):
        # Plain HTTP to a remote host must be rejected
        with self.assertRaises(ConfigError) as ctx:
            load_config(env_dict={
                "WEBDAV_BASE_URL": "http://archive.remote.corp/dav",
                "WEBDAV_USER": "u",
                "WEBDAV_PASSWORD": "p",
                "WEBDAV_FOLDER": "f",
            })
        self.assertIn("Insecure HTTP scheme is only permitted for local loopback", str(ctx.exception))

    def test_loopback_http_permitted(self):
        for host in ("127.0.0.1", "localhost"):
            cfg = load_config(env_dict={
                "WEBDAV_BASE_URL": f"http://{host}:8080/dav",
                "WEBDAV_USER": "u",
                "WEBDAV_PASSWORD": "p",
                "WEBDAV_FOLDER": "f",
            })
            self.assertEqual(cfg.webdav_user, "u")

    def test_remote_https_permitted(self):
        cfg = load_config(env_dict={
            "WEBDAV_BASE_URL": "https://archive.corp.internal/dav",
            "WEBDAV_USER": "u",
            "WEBDAV_PASSWORD": "p",
            "WEBDAV_FOLDER": "f",
        })
        self.assertTrue(cfg.webdav_base_url.startswith("https://"))

    def test_invalid_timeout_raises(self):
        with self.assertRaises(ConfigError):
            load_config(env_dict={
                "WEBDAV_BASE_URL": "http://127.0.0.1:80",
                "WEBDAV_USER": "u",
                "WEBDAV_PASSWORD": "p",
                "WEBDAV_FOLDER": "f",
                "WEBDAV_TIMEOUT": "-5",
            })

    def test_relative_state_path_converted_to_absolute(self):
        cfg = load_config(env_dict={
            "WEBDAV_BASE_URL": "http://127.0.0.1:80",
            "WEBDAV_USER": "u",
            "WEBDAV_PASSWORD": "p",
            "WEBDAV_FOLDER": "f",
            "STATE_FILE_PATH": "my_state.json",
        })
        self.assertTrue(os.path.isabs(cfg.state_file_path))
        self.assertTrue(cfg.state_file_path.endswith("my_state.json"))

    def test_destination_api_key_loaded_and_hidden_in_repr(self):
        cfg = load_config(env_dict={
            "WEBDAV_BASE_URL": "http://127.0.0.1:80",
            "WEBDAV_USER": "u",
            "WEBDAV_PASSWORD": "p",
            "WEBDAV_FOLDER": "f",
            "DESTINATION_API_KEY": "super-secret-anythingllm-api-token-999",
        })
        self.assertEqual(cfg.destination_api_key, "super-secret-anythingllm-api-token-999")
        rep = repr(cfg)
        self.assertNotIn("super-secret-anythingllm-api-token-999", rep)
        self.assertNotIn("destination_api_key", rep)

if __name__ == "__main__":
    unittest.main()
