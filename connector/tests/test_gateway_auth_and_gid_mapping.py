# -*- coding: utf-8 -*-
"""Focused Mock and Component Tests for AI Box Gateway Authentication and Access Control.

Covers all 10 mandatory architectural requirements:
1. Missing session rejected with HTTP 401 (fails closed, backend untouched).
2. Expired session or invalid OCS response rejected with HTTP 401.
3. Archive network failure / timeout causes Fail-Closed (HTTP 503).
4. Group A member successfully accesses Group A workspace.
5. Group A member blocked with HTTP 403 when attempting to access Group B workspace.
6. Multi-group user accesses only workspaces mapped to their assigned groups.
7. Unmapped workspace slug rejected with HTTP 403 (never guessed or defaulted).
8. Admin, system, and developer ingest endpoints blocked from public interface (HTTP 403).
9. Authorized chat and chunked streaming requests successfully proxy and stream.
10. Authentication and authorization failures do NOT leak session cookies or tokens to logs.
Plus:
- CORS and untrusted Origin validation.
- Filtered workspaces listing according to user groups.
- Internal connector service bypass with secret bearer token.
- GIDWorkspaceMapper atomic storage and bidirectional resolution.
"""

import http.client
import http.server
import io
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch, MagicMock
import urllib.error
import urllib.request

# Ensure imports resolve
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../gateway")))

from gid_mapper import GIDWorkspaceMapper
import proxy


def build_mock_backend(status: int = 200, headers: list = None, body_chunks: list = None):
    """Helper creating mock backend HTTPConnection."""
    mock_conn = MagicMock()
    mock_res = MagicMock()
    mock_res.status = int(status)
    mock_res.reason = "OK" if status == 200 else "Error"
    chunks = body_chunks if body_chunks is not None else [b'{"success": true}']
    total_len = sum(len(c) for c in chunks)
    mock_res.getheaders.return_value = headers or [
        ("Content-Type", "application/json"),
        ("Content-Length", str(total_len)),
    ]
    mock_res.read.side_effect = chunks + [b""]
    mock_conn.getresponse.return_value = mock_res
    return mock_conn


class GatewayAuthAndAccessControlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Configure temporary isolated GID mapping file
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.map_path = os.path.join(cls.temp_dir.name, "test_gid_workspace_map.json")
        cls.mapper = GIDWorkspaceMapper(cls.map_path)
        cls.mapper.set_mapping("Group A", "group-a", "Group A")
        cls.mapper.set_mapping("Group B", "group-b", "Group B")
        cls.mapper.set_mapping("Finance", "finance-dept", "Finance")

        # Point proxy module to test mapper
        proxy.gid_mapper = cls.mapper
        proxy.INTERNAL_SERVICE_KEY = "test-service-key-999"
        proxy.ARCHIVE_AUTH_TIMEOUT = 0.2

        # Start real test HTTP server on ephemeral port (port 0)
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), proxy.SafeAIBOXProxyHandler)
        cls.server_port = cls.server.server_address[1]
        cls.base_url = f"http://127.0.0.1:{cls.server_port}"

        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.temp_dir.cleanup()

    def setUp(self):
        proxy.session_acl_cache.clear()

    def _request(self, method: str, path: str, headers: dict = None, data: bytes = None):
        """Helper to issue HTTP request to local gateway test server."""
        url = f"{self.base_url}{path}"
        req_headers = headers.copy() if headers else {}
        if method == "POST" and any(x in path for x in ("/chat", "/stream-chat")) and "SKIP_CHAT_SECURITY_HEADERS" not in req_headers:
            req_headers.setdefault("Origin", "http://localhost")
            req_headers.setdefault("X-Archive-Chat", "true")
        req_headers.pop("SKIP_CHAT_SECURITY_HEADERS", None)
        req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                body = resp.read()
                return resp.status, resp.headers, body
        except urllib.error.HTTPError as e:
            return e.code, e.headers, e.read()

    # --- Test 1: Missing Session Rejected with 401 ---

    @patch("proxy.validate_archive_session")
    def test_01_request_without_session_rejected_401(self, mock_validate):
        """1. Request without valid session rejected with 401 and does not reach AnythingLLM."""
        mock_validate.return_value = (
            False, None, [], 401, "Authentication required: No Nextcloud session cookie provided."
        )

        status, headers, body = self._request("POST", "/api/workspace/group-a/chat", data=b'{"message":"hi"}')

        self.assertEqual(status, 401)
        resp_json = json.loads(body.decode("utf-8"))
        self.assertFalse(resp_json["success"])
        self.assertIn("Authentication required", resp_json["error"])

    # --- Test 2: Expired or Invalid OCS Response Rejected with 401 ---

    def test_02_expired_or_invalid_session_rejected_401(self):
        """2. Expired session or invalid OCS response fails closed with 401."""
        # Test validate_archive_session directly with simulated invalid OCS responses
        with patch("urllib.request.urlopen") as mock_urlopen:
            # Simulate OCS returning failure (997 Unauthorised)
            mock_resp = MagicMock()
            mock_resp.status = 200
            mock_resp.read.return_value = json.dumps({
                "ocs": {
                    "meta": {"status": "failure", "statuscode": 997, "message": "Current user is not logged in"},
                    "data": []
                }
            }).encode("utf-8")
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            ok, uid, groups, code, msg = proxy.validate_archive_session(
                {"Cookie": "oc_sessionPassphrase=expired; ockqp1rg99oy=invalid"}
            )
            self.assertFalse(ok)
            self.assertEqual(code, 401)
            self.assertIn("Current user is not logged in", msg)

        # HTTP 401 from Nextcloud
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.HTTPError(
                url="http://127.0.0.1/ocs/v1.php/cloud/user?format=json",
                code=401,
                msg="Unauthorized",
                hdrs={},
                fp=io.BytesIO(b"Unauthorized"),
            )
            ok, uid, groups, code, msg = proxy.validate_archive_session({"Cookie": "oc_sessionPassphrase=bad"})
            self.assertFalse(ok)
            self.assertEqual(code, 401)

    # --- Test 3: Network Failure or Timeout on Archive Causes 503 Fail-Closed ---

    def test_03_archive_network_timeout_causes_fail_closed_503(self):
        """3. Network error or Archive unavailability causes 503 Fail-Closed."""
        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = urllib.error.URLError("Connection refused by Enterprise Archive")

            ok, uid, groups, code, msg = proxy.validate_archive_session({"Cookie": "valid_looking_cookie=1"})
            self.assertFalse(ok)
            self.assertEqual(code, 503)
            self.assertIn("Failed to connect to Enterprise Archive", msg)

        with patch("urllib.request.urlopen") as mock_urlopen:
            mock_urlopen.side_effect = TimeoutError("Archive timed out")

            ok, uid, groups, code, msg = proxy.validate_archive_session({"Cookie": "valid_looking_cookie=1"})
            self.assertFalse(ok)
            self.assertEqual(code, 503)

    # --- Test 4: Group A User Accesses Group A Workspace ---

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_04_group_a_user_accesses_group_a_workspace(self, mock_validate, mock_create_backend):
        """4. User belonging to Group A has access to valid Group A workspace."""
        mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"text":"answer"}'])

        status, headers, body = self._request(
            "POST",
            "/api/workspace/group-a/stream-chat",
            headers={"Cookie": "nc_session=valid_a"},
            data=b'{"message": "Hello AI"}',
        )
        self.assertEqual(status, 200)
        self.assertIn(b"answer", body)
        mock_create_backend.assert_called_once()

    # --- Test 5: Group A User Cannot Access Group B Workspace by Changing Slug ---

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_05_group_a_user_blocked_from_group_b_workspace_403(self, mock_validate, mock_create_backend):
        """5. User of Group A cannot invoke Group B workspace simply by changing slug (HTTP 403)."""
        mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")

        status, headers, body = self._request(
            "POST",
            "/api/workspace/group-b/stream-chat",
            headers={"Cookie": "nc_session=valid_a"},
            data=b'{"message": "Unauthorized spy"}',
        )
        self.assertEqual(status, 403)
        resp_json = json.loads(body.decode("utf-8"))
        self.assertFalse(resp_json["success"])
        self.assertIn("Access denied", resp_json["error"])
        # Ensure backend was NEVER contacted
        mock_create_backend.assert_not_called()

    # --- Test 6: Multi-group User Only Accesses Their Mapped Workspaces ---

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_06_multi_group_user_access_control(self, mock_validate, mock_create_backend):
        """6. Multi-group user accesses only workspaces matching their own groups."""
        # User is in Group A and Finance, but NOT Group B
        mock_validate.return_value = (True, "multi_user", ["Group A", "Finance"], 200, "OK")
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"history":[]}'])

        # 1. Allowed on Group A
        status, _, _ = self._request(
            "GET", "/api/workspace/group-a/chats", headers={"Cookie": "nc_session=multi"}
        )
        self.assertEqual(status, 200)

        # 2. Allowed on Finance
        mock_create_backend.reset_mock()
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"history":[]}'])
        status, _, _ = self._request(
            "GET", "/api/workspace/finance-dept/chats", headers={"Cookie": "nc_session=multi"}
        )
        self.assertEqual(status, 200)

        # 3. Blocked on Group B
        mock_create_backend.reset_mock()
        status, _, body = self._request(
            "GET", "/api/workspace/group-b/chats", headers={"Cookie": "nc_session=multi"}
        )
        self.assertEqual(status, 403)
        mock_create_backend.assert_not_called()

    # --- Test 7: Unmapped Workspace Slug Fails Closed (No Guessing / No Default) ---

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_07_unmapped_workspace_rejected_403(self, mock_validate, mock_create_backend):
        """7. Unmapped workspace slug rejected with 403 (never falls back to default or guesses)."""
        mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")

        status, _, body = self._request(
            "POST",
            "/api/workspace/unknown-legacy-slug/chat",
            headers={"Cookie": "nc_session=valid_a"},
            data=b'{"message": "probe"}',
        )
        self.assertEqual(status, 403)
        resp_json = json.loads(body.decode("utf-8"))
        self.assertIn("not mapped to any authorized group", resp_json["error"])
        mock_create_backend.assert_not_called()

    # --- Test 8: Admin and Developer Ingest Routes Blocked from Public Access ---

    def test_08_admin_and_internal_routes_blocked_from_public(self):
        """8. Public requests to admin, system, and document upload endpoints blocked with 403."""
        blocked_paths = [
            ("GET", "/api/v1/admin/users"),
            ("GET", "/api/v1/system/env-dump"),
            ("POST", "/api/v1/workspace/new"),
            ("POST", "/api/v1/document/upload"),
            ("POST", "/api/workspace/group-a/upload-and-embed"),
            ("POST", "/api/workspace/group-a/update-embeddings"),
            ("POST", "/api/workspace/group-a/parse"),
            ("GET", "/api/workspace/group-a/upload-link"),
        ]

        for method, path in blocked_paths:
            status, _, body = self._request(method, path, headers={"Cookie": "nc_session=anyone"})
            self.assertEqual(status, 403, f"Path {path} should be blocked with 403 for public access")
            resp_json = json.loads(body.decode("utf-8"))
            self.assertFalse(resp_json["success"])

    # --- Test 9: Authorized Chat and Chunked Stream Requests Work Cleanly ---

    def test_09_authorized_chat_and_stream_forwarding(self):
        """9. Authorized chat and chunked streaming requests forward cleanly."""
        with patch("proxy.create_backend_connection") as mock_conn_cls, \
             patch("proxy.validate_archive_session") as mock_validate:

            mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")

            # Mock backend streaming response
            mock_conn = MagicMock()
            mock_res = MagicMock()
            mock_res.status = 200
            mock_res.reason = "OK"
            mock_res.getheaders.return_value = [
                ("Content-Type", "text/event-stream"),
                ("Transfer-Encoding", "chunked"),
            ]
            mock_res.read.side_effect = [b"data: chunk 1\n\n", b"data: chunk 2\n\n", b""]
            mock_conn.getresponse.return_value = mock_res
            mock_conn_cls.return_value = mock_conn

            status, headers, body = self._request(
                "POST",
                "/api/workspace/group-a/stream-chat",
                headers={"Cookie": "nc_session=test_a"},
                data=b'{"message": "Stream me"}',
            )

            self.assertEqual(status, 200)
            self.assertIn(b"chunk 1", body)
            self.assertIn(b"chunk 2", body)

    # --- Test 10: Zero-Credential Logging (No Tokens or Cookies Exposed) ---

    def test_10_zero_credential_logging(self):
        """10. Auth errors do NOT leak session cookies, passwords, or tokens in logs."""
        captured_stderr = io.StringIO()
        secret_cookie = "ockqp1rg99oy=SUPER_SECRET_SESSION_ID_XYZ; oc_sessionPassphrase=ULTRA_SECRET_TOKEN"

        with patch("sys.stderr", captured_stderr), \
             patch("proxy.validate_archive_session") as mock_validate:

            mock_validate.return_value = (False, None, [], 401, "Session expired")

            self._request(
                "POST",
                "/api/workspace/group-a/chat?token=LEAKY_TOKEN",
                headers={"Cookie": secret_cookie},
                data=b'{"message": "test"}',
            )

            log_output = captured_stderr.getvalue()
            # Verify sensitive elements NEVER appear in log output
            self.assertNotIn("SUPER_SECRET_SESSION_ID_XYZ", log_output)
            self.assertNotIn("ULTRA_SECRET_TOKEN", log_output)
            self.assertNotIn("LEAKY_TOKEN", log_output)
            self.assertNotIn("Cookie", log_output)

    # --- Additional Tests: CORS, Filtered Workspaces, and Internal Service ---

    def test_cors_trusted_origin_allowed_and_untrusted_blocked(self):
        """CORS: Trusted origin gets Allow-Origin and credentials; untrusted origin blocked on POST."""
        # 1. Preflight OPTIONS from trusted origin
        status, headers, _ = self._request(
            "OPTIONS",
            "/api/workspace/group-a/chat",
            headers={"Origin": "http://127.0.0.1:80"}
        )
        self.assertEqual(status, 204)
        self.assertEqual(headers.get("Access-Control-Allow-Origin"), "http://127.0.0.1:80")
        self.assertEqual(headers.get("Access-Control-Allow-Credentials"), "true")

        # 2. State-changing POST from untrusted origin blocked
        status, _, body = self._request(
            "POST",
            "/api/workspace/group-a/chat",
            headers={"Origin": "http://malicious-attacker.com"},
            data=b'{"message": "attack"}',
        )
        self.assertEqual(status, 403)
        self.assertIn("not authorized", body.decode("utf-8"))

    @patch("proxy.validate_archive_session")
    def test_workspaces_list_filtered_by_user_groups(self, mock_validate):
        """Workspaces listing returns only workspaces matching user's authorized groups and strips internal metadata."""
        mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")

        with patch("proxy.create_backend_connection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_res = MagicMock()
            mock_res.status = 200
            # Backend has Group A, Group B, and unmapped workspaces, with internal metadata
            mock_res.read.return_value = json.dumps({
                "workspaces": [
                    {
                        "id": 1,
                        "name": "Group A",
                        "slug": "group-a",
                        "openAiPrompt": "Confidential system prompt",
                        "similarityThreshold": 0.25,
                        "topN": 4,
                        "chatModel": "qwen2.5:0.5b-instruct",
                        "threads": [{"id": 101}],
                        "documents": [{"id": 50}],
                        "queryRefusalResponse": "Refused"
                    },
                    {
                        "id": 2,
                        "name": "Group B",
                        "slug": "group-b",
                        "openAiPrompt": "Confidential prompt B"
                    },
                    {
                        "id": 3,
                        "name": "Legacy",
                        "slug": "legacy-workspace"
                    },
                ]
            }).encode("utf-8")
            mock_conn.getresponse.return_value = mock_res
            mock_conn_cls.return_value = mock_conn

            status, _, body = self._request(
                "GET", "/api/v1/workspaces", headers={"Cookie": "nc_session=user_a"}
            )
            self.assertEqual(status, 200)
            data = json.loads(body.decode("utf-8"))
            returned_ws = data["workspaces"]
            slugs = [ws["slug"] for ws in returned_ws]

            # 1. Group & Personal Isolation: User A gets Group A + personal workspace
            self.assertIn("group-a", slugs)
            self.assertIn("user-test_user_a", slugs)

            # 2. Strict Allowlist Projection: Only id, name, and slug are permitted
            ws_a = next(w for w in returned_ws if w["slug"] == "group-a")
            self.assertEqual(set(ws_a.keys()), {"id", "name", "slug"})
            self.assertEqual(ws_a["id"], 1)
            self.assertEqual(ws_a["name"], "Group A")
            self.assertEqual(ws_a["slug"], "group-a")

            # 3. Internal fields MUST NOT leak
            self.assertNotIn("openAiPrompt", ws_a)
            self.assertNotIn("similarityThreshold", ws_a)
            self.assertNotIn("topN", ws_a)
            self.assertNotIn("chatModel", ws_a)
            self.assertNotIn("threads", ws_a)
            self.assertNotIn("documents", ws_a)
            self.assertNotIn("queryRefusalResponse", ws_a)

    @patch("proxy.validate_archive_session")
    def test_workspaces_list_multi_group_user_isolation_and_allowlist(self, mock_validate):
        """Multi-group user gets only assigned workspaces, all cleanly projected to public allowlist."""
        mock_validate.return_value = (True, "multi_user", ["Group A", "Finance"], 200, "OK")

        with patch("proxy.create_backend_connection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_res = MagicMock()
            mock_res.status = 200
            mock_res.read.return_value = json.dumps({
                "workspaces": [
                    {"id": 1, "name": "Group A", "slug": "group-a", "openAiPrompt": "secret_a"},
                    {"id": 2, "name": "Group B", "slug": "group-b", "openAiPrompt": "secret_b"},
                    {"id": 3, "name": "Finance Dept", "slug": "finance-dept", "openAiPrompt": "secret_fin"},
                ]
            }).encode("utf-8")
            mock_conn.getresponse.return_value = mock_res
            mock_conn_cls.return_value = mock_conn

            status, _, body = self._request(
                "GET", "/api/v1/workspaces", headers={"Cookie": "nc_session=multi"}
            )
            self.assertEqual(status, 200)
            data = json.loads(body.decode("utf-8"))
            returned_ws = data["workspaces"]
            slugs = [ws["slug"] for ws in returned_ws]

            # Allowed: group-a, finance-dept, and user-multi_user; Blocked: group-b
            self.assertEqual(sorted(slugs), ["finance-dept", "group-a", "user-multi_user"])
            for ws in returned_ws:
                self.assertTrue(set(ws.keys()).issubset({"id", "name", "slug"}))
                self.assertNotIn("openAiPrompt", ws)

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_personal_workspace_access_control(self, mock_validate, mock_create_backend):
        """User can access their own user-{uid} workspace, but receives 403 for another user's personal workspace."""
        mock_validate.return_value = (True, "test_user_a", ["Group A"], 200, "OK")
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"text":"answer"}'])

        # 1. Owner accessing own personal workspace -> 200
        status_own, _, _ = self._request(
            "POST",
            "/api/workspace/user-test_user_a/stream-chat",
            data=b'{"message": "Hello"}',
            headers={"Cookie": "nc_session=user_a"},
        )
        self.assertEqual(status_own, 200)

        # 2. User A accessing User B's personal workspace -> 403 Forbidden
        status_other, _, body_other = self._request(
            "POST",
            "/api/workspace/user-test_user_b/stream-chat",
            data=b'{"message": "Spy"}',
            headers={"Cookie": "nc_session=user_a"},
        )
        self.assertEqual(status_other, 403)
        self.assertIn("Personal workspace 'user-test_user_b' does not belong to user 'test_user_a'", body_other.decode())

    def test_internal_service_bypass_with_valid_key(self):
        """Internal service with Bearer INTERNAL_SERVICE_KEY can access ingestion endpoints."""
        with patch("proxy.create_backend_connection") as mock_conn_cls:
            mock_conn_cls.return_value = build_mock_backend(200, body_chunks=[b'{"workspace": {"id": 1}}'])
            status, _, _ = self._request(
                "POST",
                "/api/v1/workspace/new",
                headers={"Authorization": f"Bearer {proxy.INTERNAL_SERVICE_KEY}"},
                data=b'{"name": "NewWorkspace"}',
            )
            self.assertEqual(status, 200)
            mock_conn_cls.assert_called_once()

    def test_gid_workspace_mapper_atomic_persistence(self):
        """GIDWorkspaceMapper correctly stores, retrieves, and updates mappings atomically."""
        temp_file = os.path.join(self.temp_dir.name, "mapper_test.json")
        mapper = GIDWorkspaceMapper(temp_file)

        # Non-existent slug/gid
        self.assertIsNone(mapper.get_slug("UnknownGID"))
        self.assertIsNone(mapper.get_gid("unknown-slug"))

        # Set and get
        mapper.set_mapping("گروه مهندسی", "engineering-dept", "Engineering")
        self.assertEqual(mapper.get_slug("گروه مهندسی"), "engineering-dept")
        self.assertEqual(mapper.get_gid("engineering-dept"), "گروه مهندسی")

        # Persistence across fresh instance
        fresh_mapper = GIDWorkspaceMapper(temp_file)
        self.assertEqual(fresh_mapper.get_slug("گروه مهندسی"), "engineering-dept")


    def test_internal_service_key_cannot_access_admin_or_arbitrary_routes(self):
        """Even with valid service key, admin/system endpoints are strictly blocked."""
        status, _, body = self._request(
            "GET",
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {proxy.INTERNAL_SERVICE_KEY}"},
        )
        self.assertEqual(status, 403)
        self.assertIn("Administrative and system APIs are restricted", body.decode("utf-8"))

        status, _, body = self._request(
            "POST",
            "/api/v1/unknown-dangerous-endpoint",
            headers={"Authorization": f"Bearer {proxy.INTERNAL_SERVICE_KEY}"},
        )
        self.assertEqual(status, 403)
        self.assertIn("not permitted for internal service key", body.decode("utf-8"))

    def test_missing_or_short_service_key_rejects_internal_access(self):
        """If INTERNAL_SERVICE_KEY is unset, empty, or shorter than 16 chars, internal access fails closed."""
        original_key = proxy.INTERNAL_SERVICE_KEY
        try:
            # Short key (< 16 chars)
            proxy.INTERNAL_SERVICE_KEY = "short_key"
            status, _, _ = self._request(
                "POST",
                "/api/v1/workspace/new",
                headers={"Authorization": "Bearer short_key"},
                data=b'{"name": "test"}',
            )
            self.assertEqual(status, 403)

            # Empty key
            proxy.INTERNAL_SERVICE_KEY = ""
            status, _, _ = self._request(
                "POST",
                "/api/v1/workspace/new",
                headers={"Authorization": "Bearer "},
                data=b'{"name": "test"}',
            )
            self.assertEqual(status, 403)
        finally:
            proxy.INTERNAL_SERVICE_KEY = original_key

    def test_gid_workspace_mapper_enforces_strict_one_to_one(self):
        """GIDWorkspaceMapper rejects slug collisions and fails closed on ambiguous reverse lookups."""
        temp_file = os.path.join(self.temp_dir.name, "mapper_collision_test.json")
        mapper = GIDWorkspaceMapper(temp_file)

        mapper.set_mapping("Group Alpha", "slug-alpha")
        self.assertEqual(mapper.get_slug("Group Alpha"), "slug-alpha")

        # Attempting to assign same slug to different GID must raise ValueError
        with self.assertRaises(ValueError) as ctx:
            mapper.set_mapping("Group Beta", "slug-alpha")
        self.assertIn("Slug collision", str(ctx.exception))

        # Direct corruption simulation: multiple GIDs mapping to same slug
        corrupted_data = {
            "mappings": {
                "G1": {"slug": "collision-slug"},
                "G2": {"slug": "collision-slug"},
            }
        }
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(corrupted_data, f)

        corrupted_mapper = GIDWorkspaceMapper(temp_file)
        # Reverse lookup MUST fail closed (return None)
        self.assertIsNone(corrupted_mapper.get_gid("collision-slug"))



    # --- Session-Based ACL Caching & Synchronization Tests ---

    def test_session_acl_cache_standalone_lifecycle(self):
        """Verify SessionACLCache standalone CRUD, TTL expiry, and pruning."""
        cache = proxy.SessionACLCache(default_ttl=0.2)
        token_key = cache.hash_token("cookie:session_token_123|auth:")

        # 1. Miss initially
        self.assertIsNone(cache.get(token_key))
        self.assertNotIn(token_key, cache)
        self.assertEqual(cache.size(), 0)

        # 2. Set entry
        cache.set(
            token_key=token_key,
            user_id="alice",
            groups=["Finance"],
            workspaces=[{"slug": "finance-dept", "name": "Finance"}],
            allowed_slugs={"finance-dept", "user-alice"},
            ttl=0.1,
        )
        self.assertEqual(cache.size(), 1)
        self.assertIn(token_key, cache)

        entry = cache.get(token_key)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["user_id"], "alice")
        self.assertIn("finance-dept", entry["allowed_slugs"])

        # 3. Expiration
        time.sleep(0.15)
        self.assertIsNone(cache.get(token_key))
        self.assertEqual(cache.size(), 0)

        # 4. Prune expired
        cache.set(token_key, "alice", [], [], set(), ttl=-1.0)
        self.assertEqual(cache.prune_expired(), 1)
        self.assertEqual(cache.size(), 0)

    def test_session_acl_cache_user_invalidation(self):
        """Verify invalidate_user clears all active sessions for a specific user."""
        cache = proxy.SessionACLCache(default_ttl=3600.0)
        t1 = cache.hash_token("session_user_x_tab1")
        t2 = cache.hash_token("session_user_x_tab2")
        t3 = cache.hash_token("session_user_y_tab1")

        cache.set(t1, "user_x", ["g1"], [], {"g1"})
        cache.set(t2, "user_x", ["g1"], [], {"g1"})
        cache.set(t3, "user_y", ["g2"], [], {"g2"})
        self.assertEqual(cache.size(), 3)

        deleted = cache.invalidate_user("user_x")
        self.assertEqual(deleted, 2)
        self.assertEqual(cache.size(), 1)
        self.assertIsNone(cache.get(t1))
        self.assertIsNone(cache.get(t2))
        self.assertIsNotNone(cache.get(t3))

    @patch("proxy.create_backend_connection")
    @patch("proxy.validate_archive_session")
    def test_session_acl_login_time_sync_and_zero_roundtrip_subsequent_chat(self, mock_validate, mock_create_backend):
        """Verify login-time sync: first request fetches ACL once, subsequent chat uses cache (0 network roundtrips)."""
        mock_validate.return_value = (True, "cached_user", ["Group A"], 200, "OK")
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"text":"answer"}'])

        cookie_header = {"Cookie": "nc_session=session_cache_perf_test"}

        # Request 1: Initial session establishment (Cache Miss -> calls validate_archive_session)
        status1, _, body1 = self._request(
            "POST",
            "/api/workspace/group-a/stream-chat",
            headers=cookie_header,
            data=b'{"message": "Hello 1"}',
        )
        self.assertEqual(status1, 200)
        self.assertIn(b"answer", body1)
        self.assertEqual(mock_validate.call_count, 1)

        # Request 2: Subsequent chat message (Cache Hit -> TRUST cached ACL, 0 calls to validate_archive_session)
        mock_create_backend.reset_mock()
        mock_create_backend.return_value = build_mock_backend(200, body_chunks=[b'{"text":"answer 2"}'])

        status2, _, body2 = self._request(
            "POST",
            "/api/workspace/group-a/stream-chat",
            headers=cookie_header,
            data=b'{"message": "Hello 2"}',
        )
        self.assertEqual(status2, 200)
        self.assertIn(b"answer 2", body2)
        # CRITICAL ASSERTION: validate_archive_session was NOT called again! Zero per-request network overhead!
        self.assertEqual(mock_validate.call_count, 1)

    @patch("proxy.validate_archive_session")
    def test_session_invalidation_endpoint_clears_cache(self, mock_validate):
        """Verify /api/v1/session/invalidate immediately purges active session from cache."""
        mock_validate.return_value = (True, "test_logout_user", ["Group A"], 200, "OK")

        cookie_header = {"Cookie": "nc_session=logout_session_id"}

        # Populate cache
        status, _, _ = self._request("GET", "/api/v1/workspaces", headers=cookie_header)
        token_key = proxy.extract_session_token(cookie_header)
        self.assertIsNotNone(proxy.session_acl_cache.get(token_key))

        # Invoke Invalidation Endpoint
        status_inv, _, body_inv = self._request("POST", "/api/v1/session/invalidate", headers=cookie_header)
        self.assertEqual(status_inv, 200)
        resp_json = json.loads(body_inv.decode("utf-8"))
        self.assertTrue(resp_json["success"])

        # Cache must now be EMPTY for this token
        self.assertIsNone(proxy.session_acl_cache.get(token_key))


if __name__ == "__main__":
    unittest.main()