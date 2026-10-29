# -*- coding: utf-8 -*-
import unittest
import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from destination_adapter import (
    LocalAiBoxAdapter,
    DeliveryContract,
    DeliveryStatus,
    build_structured_filename,
)

class TestDestinationAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test")
        self.hash64 = "686f1f9a2bbc31017c303b69445d1ff3ecff95da427840c0ca491cb72ef14508"

    def test_build_structured_filename_persian_and_full_sha256(self):
        # Persian filename with spaces and half-spaces must be preserved
        fn = build_structured_filename("660", self.hash64, "گزارش\u200c نهایی پروژه.txt")
        self.assertTrue(fn.startswith(f"arch_660_{self.hash64}_"))
        self.assertTrue(fn.endswith(".txt"))
        self.assertIn("گزارش\u200c نهایی پروژه", fn)

    def test_build_structured_filename_invalid_hash_raises(self):
        with self.assertRaises(ValueError):
            build_structured_filename("660", "tooshort", "file.txt")

    def test_build_structured_filename_length_capped_in_utf8_bytes(self):
        # Very long Persian filename (each Persian char is 2 bytes in UTF-8)
        long_persian = "یک_نام_بسیار_طولانی_برای_فایل_آزمایشی_" * 15 + ".txt"
        fn = build_structured_filename("660", self.hash64, long_persian)
        self.assertTrue(fn.endswith(".txt"))
        # Entire filename MUST be <= 180 bytes in UTF-8
        encoded_bytes = fn.encode("utf-8")
        self.assertLessEqual(len(encoded_bytes), 180)
        self.assertTrue(fn.startswith(f"arch_660_{self.hash64}_"))

    @patch.object(LocalAiBoxAdapter, "inspect_workspace_documents")
    def test_detect_already_processed_document(self, mock_inspect):
        # Exact match with source ID and full hash
        expected_filename = f"arch_660_{self.hash64}_persian_doc.txt"
        mock_inspect.return_value = [
            {"filename": expected_filename, "docpath": f"custom-documents/{expected_filename}-uuid.json"}
        ]
        contract = DeliveryContract("660", "persian_doc.txt", "text/plain", self.hash64, b"content")
        result = self.adapter.deliver(contract)
        self.assertEqual(result.status, DeliveryStatus.ALREADY_PROCESSED)
        self.assertIn("already indexed", result.message)

    @patch.object(LocalAiBoxAdapter, "inspect_workspace_documents")
    def test_detect_version_conflict_marks_needs_review(self, mock_inspect):
        # Same source ID but different content hash -> MUST mark NEEDS_REVIEW
        different_hash = "1111111111111111111111111111111111111111111111111111111111111111"
        mock_inspect.return_value = [
            {"filename": f"arch_660_{different_hash}_old.txt", "docpath": f"custom-documents/arch_660_{different_hash}_old.txt-uuid.json"}
        ]
        contract = DeliveryContract("660", "persian_doc.txt", "text/plain", self.hash64, b"new content")
        result = self.adapter.deliver(contract)
        self.assertEqual(result.status, DeliveryStatus.NEEDS_REVIEW)
        self.assertIn("Auto-resend halted", result.message)

    @patch.object(LocalAiBoxAdapter, "inspect_workspace_documents")
    @patch("urllib.request.urlopen")
    def test_successful_upload_and_embed(self, mock_urlopen, mock_inspect):
        mock_inspect.return_value = []

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"success": true, "error": null, "document": {"id": "uuid-1", "location": "custom-documents/doc.json"}}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        contract = DeliveryContract("660", "my report.txt", "text/plain", self.hash64, b"plain text")
        result = self.adapter.deliver(contract)
        self.assertEqual(result.status, DeliveryStatus.SUCCESS)

    @patch.object(LocalAiBoxAdapter, "inspect_workspace_documents")
    @patch("urllib.request.urlopen")
    def test_anythingllm_embedding_failure_handling(self, mock_urlopen, mock_inspect):
        # AnythingLLM returns 200 with success: false on worker failure
        mock_inspect.return_value = []
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"success": false, "error": "Embedding worker failed", "document": null}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        contract = DeliveryContract("660", "report.txt", "text/plain", self.hash64, b"plain text")
        result = self.adapter.deliver(contract)
        self.assertEqual(result.status, DeliveryStatus.RETRYABLE_FAILURE)
        self.assertIn("Embedding worker failed", result.message)

    @patch("urllib.request.urlopen")
    def test_remove_document_from_workspace_success(self, mock_urlopen):
        """Verifies remove_document_from_workspace sends POST /api/v1/workspace/:slug/update-embeddings with deletes."""
        import json
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps({
            "message": None,
            "workspace": {"id": 1, "slug": "ai-box-test"}
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test", api_key="secret-key")
        result = adapter.remove_document_from_workspace("custom-documents/old_doc.txt-uuid.json")
        self.assertTrue(result)

        self.assertEqual(mock_urlopen.call_count, 1)
        req = mock_urlopen.call_args[0][0]
        self.assertEqual(req.full_url, "http://127.0.0.1:3001/api/v1/workspace/ai-box-test/update-embeddings")
        self.assertEqual(req.method, "POST")
        self.assertEqual(req.get_header("Authorization"), "Bearer secret-key")
        payload = json.loads(req.data.decode("utf-8"))
        self.assertEqual(payload, {"adds": [], "deletes": ["custom-documents/old_doc.txt-uuid.json"]})

    @patch("urllib.request.urlopen")
    def test_remove_document_from_workspace_http_error(self, mock_urlopen):
        """Verifies remove_document_from_workspace handles HTTP error cleanly returning False."""
        import urllib.error
        fp = MagicMock()
        fp.read.return_value = b'{"error": "Document not found"}'
        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="http://127.0.0.1:3001/api/v1/workspace/ai-box-test/update-embeddings",
            code=404,
            msg="Not Found",
            hdrs={},
            fp=fp
        )
        adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test")
        result = adapter.remove_document_from_workspace("custom-documents/missing.json")
        self.assertFalse(result)

    def test_remove_document_empty_location(self):
        """Verifies empty doc_location returns False without issuing network request."""
        adapter = LocalAiBoxAdapter()
        self.assertFalse(adapter.remove_document_from_workspace(""))
        self.assertFalse(adapter.remove_document_from_workspace(None))

    @patch("urllib.request.urlopen")
    def test_remove_document_fails_on_null_workspace_even_if_http_200(self, mock_urlopen):
        """Verifies HTTP 200 with null workspace is strictly identified as failure."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"workspace": null, "message": "Failed to update embeddings"}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test")
        result = adapter.remove_document_from_workspace("custom-documents/broken.json")
        self.assertFalse(result, "HTTP 200 with workspace:null must NOT be considered success")

    @patch("urllib.request.urlopen")
    def test_remove_document_fails_on_invalid_json_even_if_http_200(self, mock_urlopen):
        """Verifies HTTP 200 with invalid non-JSON body is treated as failure."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'<html>Internal Gateway HTML Response</html>'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test")
        result = adapter.remove_document_from_workspace("custom-documents/html.json")
        self.assertFalse(result, "Non-JSON response must be treated as failure")

    @patch("urllib.request.urlopen")
    def test_remove_document_fails_on_error_key_even_if_http_200(self, mock_urlopen):
        """Verifies HTTP 200 with explicit error message is treated as failure."""
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = b'{"error": "Database locked", "workspace": null}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        adapter = LocalAiBoxAdapter(gateway_url="http://127.0.0.1:3001", workspace_slug="ai-box-test")
        result = adapter.remove_document_from_workspace("custom-documents/locked.json")
        self.assertFalse(result)

if __name__ == "__main__":
    unittest.main()
