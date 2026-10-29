# -*- coding: utf-8 -*-
import unittest
import os
import sys
import json
import urllib.error
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gateway")))

from destination_adapter import (
    LocalAiBoxAdapter,
    WorkspaceResolutionError,
    AmbiguousWorkspaceError,
    build_structured_filename,
    SUPPORTED_TEXT_EXTENSIONS,
)
from source_webdav import validate_supported_text_file, validate_text_file
import proxy

class TestGroupWorkspaceAndTextFormats(unittest.TestCase):
    def setUp(self):
        self.adapter = LocalAiBoxAdapter(
            gateway_url="http://127.0.0.1:3001",
            workspace_slug="ai-box-test",
            api_key="test-secret-key-12345"
        )
        self.unauth_adapter = LocalAiBoxAdapter(
            gateway_url="http://127.0.0.1:3001",
            workspace_slug="ai-box-test",
            api_key=None
        )
        self.blank_key_adapter = LocalAiBoxAdapter(
            gateway_url="http://127.0.0.1:3001",
            workspace_slug="ai-box-test",
            api_key="   "
        )
        self.hash64 = "686f1f9a2bbc31017c303b69445d1ff3ecff95da427840c0ca491cb72ef14508"

    # --- Authentication Guard Tests (Safe Halt Before Issuing Requests) ---

    @patch("urllib.request.urlopen")
    def test_unauthenticated_adapter_halts_before_request_on_list(self, mock_urlopen):
        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.unauth_adapter.list_workspaces()
        mock_urlopen.assert_not_called()
        self.assertIn("Destination API key is missing or empty", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_unauthenticated_adapter_halts_before_request_on_create(self, mock_urlopen):
        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.unauth_adapter.create_workspace("Test Group")
        mock_urlopen.assert_not_called()
        self.assertIn("Destination API key is missing or empty", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_blank_key_adapter_halts_before_request_on_list(self, mock_urlopen):
        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.blank_key_adapter.list_workspaces()
        mock_urlopen.assert_not_called()
        self.assertIn("Destination API key is missing or empty", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_blank_key_adapter_halts_before_request_on_create(self, mock_urlopen):
        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.blank_key_adapter.create_workspace("Test Group")
        mock_urlopen.assert_not_called()
        self.assertIn("Destination API key is missing or empty", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_unauthenticated_adapter_halts_on_resolve_or_create(self, mock_urlopen):
        with self.assertRaises(WorkspaceResolutionError):
            self.unauth_adapter.resolve_or_create_group_workspace("Test Group")
        mock_urlopen.assert_not_called()

    # --- Group Workspace Resolution Tests & V1 Endpoints ---

    @patch("urllib.request.urlopen")
    def test_find_existing_workspace_exact_match_v1_api(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "workspaces": [
                {"id": 1, "name": "General", "slug": "general"},
                {"id": 2, "name": "دپارتمان مالی", "slug": "dpartman-maly"},
                {"id": 3, "name": "Engineering", "slug": "engineering"},
            ]
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        slug = self.adapter.resolve_or_create_group_workspace("دپارتمان مالی")
        self.assertEqual(slug, "dpartman-maly")

        # Verify exact request URL and Bearer Authorization header
        call_args, _ = mock_urlopen.call_args
        req = call_args[0]
        self.assertEqual(req.full_url, "http://127.0.0.1:3001/api/v1/workspaces")
        self.assertEqual(req.get_header("Authorization"), "Bearer test-secret-key-12345")
        self.assertEqual(req.get_header("Accept"), "application/json")

    @patch("urllib.request.urlopen")
    def test_create_workspace_when_not_found_v1_api(self, mock_urlopen):
        # First call: list_workspaces returns no match
        # Second call: create_workspace returns created workspace
        resp_list = MagicMock()
        resp_list.read.return_value = json.dumps({
            "workspaces": [{"id": 1, "name": "General", "slug": "general"}]
        }).encode("utf-8")

        resp_create = MagicMock()
        resp_create.read.return_value = json.dumps({
            "message": None,
            "workspace": {
                "id": 10,
                "name": "منابع انسانی",
                "slug": "mnabe-ansany-random123"
            }
        }).encode("utf-8")

        mock_urlopen.return_value.__enter__.side_effect = [resp_list, resp_create]

        slug = self.adapter.resolve_or_create_group_workspace("منابع انسانی")
        self.assertEqual(slug, "mnabe-ansany-random123")

        self.assertEqual(mock_urlopen.call_count, 2)
        # Check first request (list)
        first_req = mock_urlopen.call_args_list[0][0][0]
        self.assertEqual(first_req.full_url, "http://127.0.0.1:3001/api/v1/workspaces")

        # Check second request (create)
        second_req = mock_urlopen.call_args_list[1][0][0]
        self.assertEqual(second_req.full_url, "http://127.0.0.1:3001/api/v1/workspace/new")
        self.assertEqual(second_req.method, "POST")
        self.assertEqual(second_req.get_header("Authorization"), "Bearer test-secret-key-12345")
        payload = json.loads(second_req.data.decode("utf-8"))
        self.assertEqual(payload, {"name": "منابع انسانی"})

    def test_halt_on_empty_or_whitespace_group_name(self):
        with self.assertRaises(ValueError):
            self.adapter.resolve_or_create_group_workspace("")

        with self.assertRaises(ValueError):
            self.adapter.resolve_or_create_group_workspace("   ")

        with self.assertRaises(ValueError):
            self.adapter.resolve_or_create_group_workspace(None)

    @patch("urllib.request.urlopen")
    def test_halt_on_ambiguous_workspace_match(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "workspaces": [
                {"id": 2, "name": "Legal", "slug": "legal-1"},
                {"id": 5, "name": "Legal", "slug": "legal-2"}
            ]
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        with self.assertRaises(AmbiguousWorkspaceError) as ctx:
            self.adapter.resolve_or_create_group_workspace("Legal")
        self.assertIn("Multiple workspaces", str(ctx.exception))
        self.assertIn("Halting", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_halt_on_http_401_or_403_auth_error(self, mock_urlopen):
        fp = MagicMock()
        fp.read.return_value = b'{"error": "Invalid API key"}'
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://127.0.0.1:3001/api/v1/workspaces",
            code=403,
            msg="Forbidden",
            hdrs={},
            fp=fp
        )
        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.adapter.resolve_or_create_group_workspace("Finance")
        self.assertIn("Authentication failed", str(ctx.exception))
        self.assertIn("HTTP 403", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_halt_on_invalid_list_response_structure_and_no_creation(self, mock_urlopen):
        # Incomplete structure without 'workspaces' key
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"unexpected_key": []}).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.adapter.resolve_or_create_group_workspace("Finance")
        self.assertIn("missing 'workspaces' root key", str(ctx.exception))
        # Ensure it halted and did NOT proceed to create_workspace
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch("urllib.request.urlopen")
    def test_halt_on_api_network_failure(self, mock_urlopen):
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        with self.assertRaises(ConnectionError):
            self.adapter.resolve_or_create_group_workspace("Finance")

    @patch("urllib.request.urlopen")
    def test_halt_on_missing_slug_in_existing_workspace(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "workspaces": [{"id": 2, "name": "Marketing", "slug": None}]
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.adapter.resolve_or_create_group_workspace("Marketing")
        self.assertIn("missing or empty slug", str(ctx.exception))

    @patch("urllib.request.urlopen")
    def test_halt_on_missing_slug_in_created_workspace(self, mock_urlopen):
        resp_list = MagicMock()
        resp_list.read.return_value = json.dumps({"workspaces": []}).encode("utf-8")

        resp_create = MagicMock()
        resp_create.read.return_value = json.dumps({
            "message": None,
            "workspace": {"id": 12, "name": "NewGroup", "slug": ""}
        }).encode("utf-8")

        mock_urlopen.return_value.__enter__.side_effect = [resp_list, resp_create]

        with self.assertRaises(WorkspaceResolutionError) as ctx:
            self.adapter.resolve_or_create_group_workspace("NewGroup")
        self.assertIn("missing or empty slug", str(ctx.exception))

    # --- Structured Filename and Extensions ---

    def test_build_structured_filename_preserves_supported_extensions(self):
        extensions = [".txt", ".md", ".log", ".csv", ".json", ".xml", ".yaml", ".yml"]
        for ext in extensions:
            fn = build_structured_filename("700", self.hash64, f"sample_doc{ext}")
            self.assertTrue(fn.startswith(f"arch_700_{self.hash64}_"), f"Failed prefix for {ext}")
            self.assertTrue(fn.endswith(ext), f"Expected {ext} suffix, got {fn}")

    # --- Gateway File Validation Tests ---

    def test_gateway_accepts_all_supported_text_formats(self):
        content = ("ستون۱,ستون۲\n" + "داده۱,داده۲").encode("utf-8")
        for ext in SUPPORTED_TEXT_EXTENSIONS:
            ok, err = proxy.validate_uploaded_file(f"test_file{ext}", content)
            self.assertTrue(ok, f"Gateway rejected valid text extension {ext}: {err}")

    def test_gateway_rejects_unsupported_extensions(self):
        content = b"Some valid UTF-8 text content"
        for ext in [".pdf", ".docx", ".zip", ".png", ".jpg", ".exe", ".bin"]:
            ok, err = proxy.validate_uploaded_file(f"file{ext}", content)
            self.assertFalse(ok, f"Gateway should reject {ext}")
            self.assertIn("Extension not permitted", err)

    def test_gateway_rejects_disguised_pdf_and_zip(self):
        ok, err = proxy.validate_uploaded_file("notes.txt", b"%PDF-1.5 evil binary")
        self.assertFalse(ok)
        self.assertIn("Disguised PDF", err)

        ok, err = proxy.validate_uploaded_file("report.md", b"PK disguised docx/zip")
        self.assertFalse(ok)
        self.assertIn("Disguised ZIP/DOCX", err)

    def test_gateway_rejects_nul_bytes_and_invalid_utf8(self):
        ok, err = proxy.validate_uploaded_file("data.json", b"{" + bytes([0]) + b"bad}")
        self.assertFalse(ok)
        self.assertIn("NUL bytes", err)

        ok, err = proxy.validate_uploaded_file("data.json", b"{\xff\xfe: 'bad'}")
        self.assertFalse(ok)
        self.assertIn("UTF-8", err)

    # --- Folder Scan Non-TXT Rejection (Backward Compatibility) ---

    def test_legacy_validate_text_file_still_strictly_txt_only(self):
        content = "valid text".encode("utf-8")
        self.assertTrue(validate_text_file("file.txt", content).is_valid)
        self.assertFalse(validate_text_file("file.md", content).is_valid)
        self.assertFalse(validate_text_file("file.csv", content).is_valid)
        self.assertFalse(validate_text_file("file.json", content).is_valid)

if __name__ == "__main__":
    unittest.main()
