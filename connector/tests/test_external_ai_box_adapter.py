# -*- coding: utf-8 -*-
"""Unit tests verifying External AI Box Plug-and-Play Adapter Contracts."""

import json
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from destination_adapter import (
    LocalAiBoxAdapter,
    ExternalAIBoxEngineAdapter,
    AnythingLLMEngineAdapter,
    DeliveryContract,
    DeliveryStatus,
)


class TestExternalAIBoxAdapter(unittest.TestCase):
    def setUp(self):
        self.adapter = LocalAiBoxAdapter(
            gateway_url="http://127.0.0.1:3001",
            workspace_slug="folder-sec",
            api_key="secret-ext-key",
            backend_provider="external_ai_box",
        )

    def test_adapter_initializes_external_engine(self):
        self.assertIsInstance(self.adapter.engine, ExternalAIBoxEngineAdapter)

    @patch("urllib.request.urlopen")
    def test_contract_1_workspace_auto_provisioning(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 201
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        ok = self.adapter.ensure_workspace_exists("folder-sec", "امنیت سایبری")
        self.assertTrue(ok)

        # Verify HTTP call matches Contract 1: POST /api/v1/workspaces with slug & name
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        self.assertEqual(req.full_url, "http://127.0.0.1:3001/api/v1/workspaces")
        self.assertEqual(req.method, "POST")
        self.assertEqual(req.headers.get("Authorization"), "Bearer secret-ext-key")
        body = json.loads(req.data.decode("utf-8"))
        self.assertEqual(body["slug"], "folder-sec")
        self.assertEqual(body["name"], "امنیت سایبری")

    @patch.object(LocalAiBoxAdapter, "check_existing_document")
    @patch("urllib.request.urlopen")
    def test_contract_2_document_ingestion_and_embedding(self, mock_urlopen, mock_check):
        mock_check.return_value = (None, None)
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps({
            "status": "indexed",
            "document_id": "doc_ext_999",
            "workspace_slug": "folder-sec",
        }).encode("utf-8")
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        contract = DeliveryContract(
            source_file_id="888",
            file_name="security_plan.txt",
            mime_type="text/plain",
            content_hash="686f1f9a2bbc31017c303b69445d1ff3ecff95da427840c0ca491cb72ef14508",
            file_payload=b"Cybersecurity directive 2026",
            workspace_slug="folder-sec",
        )

        result = self.adapter.deliver(contract)
        self.assertEqual(result.status, DeliveryStatus.SUCCESS)
        self.assertEqual(result.doc_location, "doc_ext_999")

        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        self.assertEqual(req.full_url, "http://127.0.0.1:3001/api/v1/workspaces/folder-sec/documents")
        self.assertEqual(req.method, "POST")
        self.assertIn("multipart/form-data", req.headers.get("Content-type", ""))
        self.assertEqual(req.headers.get("X-source-file-id"), "888")
        self.assertEqual(req.headers.get("X-content-sha256"), "686f1f9a2bbc31017c303b69445d1ff3ecff95da427840c0ca491cb72ef14508")

    @patch("urllib.request.urlopen")
    def test_contract_3_document_vector_deletion(self, mock_urlopen):
        mock_resp = MagicMock()
        mock_resp.status = 204
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        ok = self.adapter.remove_document_from_workspace("doc_ext_999", "folder-sec")
        self.assertTrue(ok)

        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        self.assertEqual(req.full_url, "http://127.0.0.1:3001/api/v1/workspaces/folder-sec/documents/doc_ext_999")
        self.assertEqual(req.method, "DELETE")
        self.assertEqual(req.headers.get("Authorization"), "Bearer secret-ext-key")


if __name__ == "__main__":
    unittest.main()
