# -*- coding: utf-8 -*-
"""Safe AI Box Gateway Proxy: Access Control, Group Authorization, and Ingestion Security."""

import hashlib
import http.client
import http.server
import json
import logging
import os
import re
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Dict, Any, Optional, Tuple, List, Set

# Configuration
AI_BACKEND_PROVIDER = os.environ.get("AI_BACKEND_PROVIDER", "anythingllm").lower().strip()
AI_BACKEND_PROTOCOL = os.environ.get("AI_BACKEND_PROTOCOL", "http").lower().strip()
BACKEND_HOST = os.environ.get("BACKEND_HOST", "anythingllm").strip()
BACKEND_PORT = int(os.environ.get("BACKEND_PORT", "3001"))
GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "3001"))
MAX_FILE_SIZE = int(os.environ.get("MAX_FILE_SIZE_BYTES", str(2 * 1024 * 1024)))  # 2 MiB default

ARCHIVE_OCS_URL = os.environ.get("ARCHIVE_OCS_URL", "http://192.168.0.1:80").rstrip("/")
ARCHIVE_HOST_HEADER = os.environ.get("ARCHIVE_HOST_HEADER", "localhost")
ARCHIVE_AUTH_TIMEOUT = float(os.environ.get("ARCHIVE_AUTH_TIMEOUT", "3.0"))
SESSION_ACL_CACHE_TTL = float(os.environ.get("SESSION_ACL_CACHE_TTL", "3600.0"))
INTERNAL_SERVICE_KEY = (
    os.environ.get("DESTINATION_API_KEY", "").strip()
    or os.environ.get("INTERNAL_SERVICE_KEY", "").strip()
)
GID_MAP_PATH = os.environ.get("GID_MAP_PATH", "./data/gid_workspace_map.json")

# CORS Allowed Origins
RAW_ALLOWED_ORIGINS = os.environ.get(
    "ALLOWED_ORIGINS",
    "http://127.0.0.1,http://localhost,http://127.0.0.1:80,http://localhost:80,http://docs.maskan,http://docs.maskan:80,http://127.0.0.1:3001,http://localhost:3001",
)
ALLOWED_ORIGINS: Set[str] = {
    orig.strip().rstrip("/").lower() for orig in RAW_ALLOWED_ORIGINS.split(",") if orig.strip()
}

# Supported text extensions for secure ingestion
SUPPORTED_TEXT_EXTENSIONS = (
    ".txt", ".md", ".log", ".csv", ".json", ".xml", ".yaml", ".yml"
)

# Allowed public fields for workspace listing (strictly minimal projection for client UI)
ALLOWED_WORKSPACE_PUBLIC_FIELDS: Tuple[str, ...] = ("id", "name", "slug")

# Prohibited routes (never permitted in AI Box)
PROHIBITED_PATH_PATTERNS = [
    re.compile(r"/upload-link", re.IGNORECASE),
    re.compile(r"/raw-text", re.IGNORECASE),
    re.compile(r"/process-link", re.IGNORECASE),
    re.compile(r"/web-scrape", re.IGNORECASE),
    re.compile(r"/youtube", re.IGNORECASE),
]

# Admin/System routes prohibited for public users
ADMIN_SYSTEM_PATTERNS = [
    re.compile(r"^/(?:api/)?v1/admin(?:/.*)?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?v1/system(?:/.*)?$", re.IGNORECASE),
]

# Internal document and workspace ingestion routes restricted to internal connector
INTERNAL_INGEST_PATTERNS = [
    re.compile(r"^/(?:api/)?v1/workspace/new/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?v1/document/.*$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/update-embeddings/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/upload(?:-and-embed)?/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/parse/?$", re.IGNORECASE),
]

# Upload routes requiring multipart content inspection
UPLOAD_PATH_PATTERNS = [
    re.compile(r"^/(?:api/)?workspace/[^/]+/upload(?:-and-embed)?/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/parse/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?v1/document/upload/?$", re.IGNORECASE),
]

# Route patterns for user-accessible workspace operations
WORKSPACE_USER_PATH_PATTERN = re.compile(
    r"^/(?:api/)?(?:v1/)?workspace/(?P<slug>[^/]+)(?P<subpath>/.*)?$",
    re.IGNORECASE,
)
WORKSPACES_LIST_PATTERN = re.compile(
    r"^/(?:api/)?v1/workspaces/?$",
    re.IGNORECASE,
)
ALLOWED_INTERNAL_PATTERNS = [
    re.compile(r"^/(?:api/)?v1/workspaces/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?v1/workspace/new/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/upload(?:-and-embed)?/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?v1/workspace/[^/]+/update-embeddings/?$", re.IGNORECASE),
    re.compile(r"^/(?:api/)?workspace/[^/]+/?$", re.IGNORECASE),
]

STATIC_ASSET_PATTERN = re.compile(
    r"^(?:/$|/index\.html$|/assets/.*|/favicon\.ico$|/manifest\.json$|/robots\.txt$|/api/ping/?$)",
    re.IGNORECASE,
)

# Route patterns for session invalidation / logout and explicit sync
SESSION_INVALIDATE_PATTERN = re.compile(
    r"^/(?:api/)?v1/(?:session/invalidate|acl/invalidate|auth/logout)(?:/.*)?$|^/logout/?$",
    re.IGNORECASE,
)
SESSION_SYNC_PATTERN = re.compile(
    r"^/(?:api/)?v1/(?:session/sync|acl/sync)(?:/.*)?$",
    re.IGNORECASE,
)


from gid_mapper import GIDWorkspaceMapper

# Global GID mapper instance
gid_mapper = GIDWorkspaceMapper()


def validate_archive_session(
    headers: Dict[str, str],
    ocs_base_url: str = ARCHIVE_OCS_URL,
    host_header: str = ARCHIVE_HOST_HEADER,
    timeout: float = ARCHIVE_AUTH_TIMEOUT,
) -> Tuple[bool, Optional[str], List[str], int, str]:
    """Validates user session against Enterprise Archive native OCS endpoint.

    Target: GET /ocs/v1.php/cloud/user?format=json
    Headers required: OCS-APIRequest: true, Accept: application/json

    Returns: (is_authenticated, user_id, list_of_group_gids, http_status_code, error_message)

    Guarantees:
    - Missing session -> 401
    - Expired/invalid session -> 401
    - Internal OCS failure -> 401
    - Timeout/network error -> 503
    - Credentials are NEVER logged
    """
    cookie_str = headers.get("Cookie", "").strip()
    auth_header = headers.get("Authorization", "").strip()

    # Reject immediately if no session credentials provided
    if not cookie_str and not auth_header:
        return False, None, [], 401, "Authentication required: No Nextcloud session cookie or Authorization header provided."

    req_url = f"{ocs_base_url}/ocs/v1.php/cloud/user?format=json"
    req_headers = {
        "OCS-APIRequest": "true",
        "Accept": "application/json",
        "Host": host_header,
        "User-Agent": "AIBox-Gateway/1.0",
    }
    if cookie_str:
        req_headers["Cookie"] = cookie_str
    if auth_header:
        req_headers["Authorization"] = auth_header

    req = urllib.request.Request(req_url, headers=req_headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status_code = resp.status
            resp_body = resp.read().decode("utf-8", errors="ignore")
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return False, None, [], 401, f"Enterprise Archive rejected session (HTTP {e.code}): Unauthorised."
        return False, None, [], 503, f"Enterprise Archive returned error (HTTP {e.code}): Service temporarily unavailable."
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return False, None, [], 503, f"Failed to connect to Enterprise Archive for authentication: {type(e).__name__}."

    if status_code != 200:
        return False, None, [], 401, f"Enterprise Archive returned HTTP {status_code}."

    try:
        data = json.loads(resp_body)
    except json.JSONDecodeError:
        return False, None, [], 503, "Enterprise Archive returned invalid non-JSON response."

    if not isinstance(data, dict):
        return False, None, [], 503, "Enterprise Archive returned malformed response structure."

    ocs = data.get("ocs")
    if not isinstance(ocs, dict):
        return False, None, [], 503, "Enterprise Archive response missing 'ocs' envelope."

    meta = ocs.get("meta", {})
    ocs_status = meta.get("status")
    ocs_statuscode = meta.get("statuscode")

    # In OCS v1: statuscode 100 == success; in OCS v2: statuscode 200 == success
    if ocs_status != "ok" or ocs_statuscode not in (100, 200):
        msg = meta.get("message") or f"OCS statuscode {ocs_statuscode}"
        return False, None, [], 401, f"Enterprise Archive authentication failed: {msg}."

    user_info = ocs.get("data")
    if not isinstance(user_info, dict):
        return False, None, [], 401, "Enterprise Archive returned empty user data."

    uid = user_info.get("id")
    if not uid or not isinstance(uid, str) or not uid.strip():
        return False, None, [], 401, "Enterprise Archive response missing valid user ID."

    groups = user_info.get("groups")
    if not isinstance(groups, list):
        groups = []

    clean_groups = [str(g).strip() for g in groups if g and str(g).strip()]

    return True, str(uid).strip(), clean_groups, 200, "OK"


def create_backend_connection(host: str, port: int, timeout: float = 300.0) -> http.client.HTTPConnection:
    return http.client.HTTPConnection(host, port, timeout=timeout)


def parse_multipart_boundary(content_type: str) -> Optional[bytes]:
    for param in content_type.split(";"):
        param = param.strip()
        if param.lower().startswith("boundary="):
            b = param[len("boundary="):].strip("\"'\t ")
            return b.encode("latin1")
    return None


def validate_uploaded_file(filename: str, content: bytes) -> Tuple[bool, Optional[str]]:
    lower_fn = filename.lower()
    if not any(lower_fn.endswith(ext) for ext in SUPPORTED_TEXT_EXTENSIONS):
        allowed_str = ", ".join(SUPPORTED_TEXT_EXTENSIONS)
        return False, f"File '{filename}' rejected: Extension not permitted. Allowed text extensions: {allowed_str}."

    if len(content) > MAX_FILE_SIZE:
        return False, f"File '{filename}' rejected: File size ({len(content)} bytes) exceeds maximum limit of {MAX_FILE_SIZE} bytes."

    if content.startswith(b"%PDF") or b"%PDF-" in content[:1024]:
        return False, f"File '{filename}' rejected: Disguised PDF content detected. PDFs are not allowed."

    if content.startswith(b"PK\x03\x04"):
        return False, f"File '{filename}' rejected: Disguised ZIP/DOCX content detected. Binary packages are not allowed."

    if b"\x00" in content:
        return False, f"File '{filename}' rejected: Binary content (NUL bytes) detected. Only plain text is allowed."

    try:
        content.decode("utf-8")
    except UnicodeDecodeError as e:
        return False, f"File '{filename}' rejected: Content is not valid UTF-8 text ({e})."

    return True, None


def inspect_multipart_body(body: bytes, boundary: bytes) -> Tuple[bool, Optional[str]]:
    delimiter = b"--" + boundary
    parts = body.split(delimiter)
    for part in parts:
        if not part or part == b"--" or part == b"--\r\n" or part == b"\r\n":
            continue
        if part.startswith(b"\r\n"):
            part = part[2:]
        if part.endswith(b"\r\n"):
            part = part[:-2]

        split_idx = part.find(b"\r\n\r\n")
        if split_idx == -1:
            continue
        header_bytes = part[:split_idx]
        file_content = part[split_idx + 4:]

        header_str = header_bytes.decode("latin1", errors="ignore")
        match = re.search(r"filename=([^;\r\n]+)", header_str, re.IGNORECASE)
        if match:
            raw_fn = match.group(1).strip("\"'\t ")
            try:
                fn = urllib.parse.unquote(raw_fn)
            except Exception:
                fn = raw_fn
            if not fn:
                continue
            ok, err_msg = validate_uploaded_file(fn, file_content)
            if not ok:
                return False, err_msg
    return True, None


def sanitize_uid_to_slug(uid: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9_-]", "-", str(uid).strip().lower())
    clean = re.sub(r"-+", "-", clean).strip("-_")
    return clean or "unknown"

def get_personal_workspace_slug(uid: str) -> str:
    return f"user-{sanitize_uid_to_slug(uid)}"


def fetch_user_archive_workspaces(
    headers: Dict[str, str],
    ocs_base_url: str = ARCHIVE_OCS_URL,
    host_header: str = ARCHIVE_HOST_HEADER,
    timeout: float = ARCHIVE_AUTH_TIMEOUT,
    user_id: Optional[str] = None,
) -> Tuple[bool, List[Dict[str, Any]], str]:
    """Queries Nextcloud Archive Dynamic Folder ACL endpoint for real-time accessible workspaces.

    Target: GET /index.php/apps/archive_autotag/api/v1/acl/workspaces
    """
    cookie_str = headers.get("Cookie", "").strip()
    auth_header = headers.get("Authorization", "").strip()

    req_url = f"{ocs_base_url}/index.php/apps/archive_autotag/api/v1/acl/workspaces"
    if user_id:
        req_url += f"?user_id={urllib.parse.quote(str(user_id))}"

    req_headers = {
        "Accept": "application/json",
        "Host": host_header,
        "User-Agent": "AIBox-Gateway/1.0",
    }
    if cookie_str:
        req_headers["Cookie"] = cookie_str
    if auth_header:
        req_headers["Authorization"] = auth_header

    req = urllib.request.Request(req_url, headers=req_headers, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status_code = resp.status
            body_bytes = resp.read()
            data = json.loads(body_bytes.decode("utf-8", errors="ignore"))
    except urllib.error.HTTPError as e:
        err_msg = e.read().decode("utf-8", errors="ignore")
        return False, [], f"Archive ACL rejected (HTTP {e.code}): {err_msg}"
    except Exception as e:
        return False, [], f"Archive ACL connection error: {e}"

    if status_code != 200 or not isinstance(data, dict):
        return False, [], f"Invalid Archive ACL response (HTTP {status_code})"

    workspaces = data.get("workspaces", [])
    return True, workspaces if isinstance(workspaces, list) else [], ""


class SessionACLCache:
    """Thread-safe in-memory cache for user sessions and authorized workspace ACLs.

    Eliminates redundant per-request network calls to Nextcloud during chat / stream-chat.
    ACLs are synchronized once upon initial session establishment and trusted for the session duration.
    """

    def __init__(self, default_ttl: float = SESSION_ACL_CACHE_TTL):
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self.default_ttl = default_ttl

    @staticmethod
    def hash_token(token_material: str) -> str:
        """Generates a secure SHA-256 hash of session credentials so sensitive cookies are not stored as keys."""
        return hashlib.sha256(token_material.encode("utf-8")).hexdigest()

    def get(self, token_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves cached session entry if present and not expired."""
        with self._lock:
            entry = self._cache.get(token_key)
            if not entry:
                return None
            now = time.time()
            if now > entry["expires_at"]:
                del self._cache[token_key]
                return None
            entry["last_accessed"] = now
            return entry

    def set(
        self,
        token_key: str,
        user_id: str,
        groups: List[str],
        workspaces: List[Dict[str, Any]],
        allowed_slugs: Set[str],
        ttl: Optional[float] = None,
    ) -> None:
        """Stores or updates session ACL entry."""
        with self._lock:
            now = time.time()
            actual_ttl = ttl if ttl is not None else self.default_ttl
            self._cache[token_key] = {
                "user_id": user_id,
                "groups": list(groups),
                "workspaces": list(workspaces),
                "allowed_slugs": set(allowed_slugs),
                "created_at": now,
                "last_accessed": now,
                "expires_at": now + actual_ttl,
            }

    def invalidate(self, token_key: str) -> bool:
        """Invalidates a specific session by token key."""
        with self._lock:
            return bool(self._cache.pop(token_key, None))

    def invalidate_user(self, user_id: str) -> int:
        """Invalidates all sessions belonging to a specific user (e.g. global logout)."""
        with self._lock:
            keys_to_del = [k for k, v in self._cache.items() if v.get("user_id") == user_id]
            for k in keys_to_del:
                del self._cache[k]
            return len(keys_to_del)

    def clear(self) -> None:
        """Clears entire cache."""
        with self._lock:
            self._cache.clear()

    def prune_expired(self) -> int:
        """Prunes expired entries to prevent memory leak over time."""
        with self._lock:
            now = time.time()
            expired_keys = [k for k, v in self._cache.items() if now > v["expires_at"]]
            for k in expired_keys:
                del self._cache[k]
            return len(expired_keys)

    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    def __contains__(self, token_key: str) -> bool:
        with self._lock:
            return self.get(token_key) is not None


# Global Session ACL Cache instance
session_acl_cache = SessionACLCache()


def extract_session_token(headers: Dict[str, str]) -> Optional[str]:
    """Extracts and hashes session credentials from request headers, strictly segregated by session."""
    cookie = headers.get("Cookie", "").strip() or headers.get("cookie", "").strip()
    auth = headers.get("Authorization", "").strip() or headers.get("authorization", "").strip()
    if not cookie and not auth:
        return None
    # Extract distinct Nextcloud session cookie token if present
    sid_match = re.search(r'(?:nc_session_id|oc_sessionid)=([^;]+)', cookie)
    nc_sid = sid_match.group(1).strip() if sid_match else ""
    token_material = f"sid:{nc_sid}|c:{cookie}|a:{auth}"
    return SessionACLCache.hash_token(token_material)


def get_or_sync_session_acl(
    headers: Dict[str, str],
    force_refresh: bool = False,
) -> Tuple[bool, Optional[str], List[str], Set[str], List[Dict[str, Any]], int, str]:
    """Retrieves authenticated user session and authorized workspaces from in-memory cache,
    or synchronizes with Nextcloud once upon initial session establishment.

    Returns:
        (is_authorized, user_id, groups, allowed_slugs, workspaces, status_code, error_message)
    """
    session_token = extract_session_token(headers)
    if not session_token:
        return (
            False,
            None,
            [],
            set(),
            [],
            401,
            "Authentication required: No Nextcloud session cookie or Authorization header provided.",
        )

    if not force_refresh:
        cached = session_acl_cache.get(session_token)
        if cached:
            return (
                True,
                cached["user_id"],
                cached["groups"],
                cached["allowed_slugs"],
                cached["workspaces"],
                200,
                "OK",
            )

    # Cache miss or explicit refresh -> Authenticate session against Enterprise Archive
    is_auth, uid, groups, err_code, err_msg = validate_archive_session(headers)
    if not is_auth:
        session_acl_cache.invalidate(session_token)
        return False, None, [], set(), [], err_code, err_msg

    # Invalidate stale cache entry if user identity changed on this token
    prev_cached = session_acl_cache.get(session_token)
    if prev_cached and prev_cached.get("user_id") != uid:
        session_acl_cache.invalidate(session_token)

    # Session authenticated! Now fetch dynamic ACL mappings live from Nextcloud
    is_acl_ok, archive_workspaces, acl_err = fetch_user_archive_workspaces(headers, user_id=uid)
    allowed_slugs = set()
    output_workspaces = []

    is_admin = (uid == "admin" or "admin" in groups)

    if is_acl_ok and archive_workspaces:
        for w in archive_workspaces:
            if isinstance(w, dict) and w.get("slug"):
                w_slug = str(w.get("slug", "")).strip()
                w_name = str(w.get("name", "")).strip()

                # Guard: Enterprise_Archive is strictly restricted to admin
                is_ea = (
                    w_name.lower() == "enterprise_archive"
                    or w_slug.lower() == "enterprise_archive"
                    or "enterprise_archive" in w_slug.lower()
                    or "enterprise_archive" in w_name.lower()
                    or w_slug.endswith("-346")
                )
                if is_ea and not is_admin:
                    sys.stderr.write(
                        f"[GATEWAY ACL GUARD] Stripped admin workspace '{w_name}' ({w_slug}) from regular user '{uid}'.\n"
                    )
                    continue

                allowed_slugs.add(w_slug)
                output_workspaces.append(w)
                f_name = w.get("name")
                if f_name:
                    mapped = gid_mapper.get_slug(f_name)
                    if mapped:
                        allowed_slugs.add(mapped)
    else:
        # Fallback to group membership and personal workspace for resilience/testing
        for g in groups:
            if g.lower() == "admin" and not is_admin:
                continue
            mapped = gid_mapper.get_slug(g)
            if mapped:
                allowed_slugs.add(mapped)
                output_workspaces.append({"slug": mapped, "name": g, "scope": "folder", "folder_id": 0})

    # Always ensure personal workspace variants for authenticated owner
    personal_clean = sanitize_uid_to_slug(uid)
    allowed_slugs.add(f"personal-{personal_clean}")
    allowed_slugs.add(f"user-{personal_clean}")
    personal_slug = get_personal_workspace_slug(uid)
    if not any(w.get("slug") == personal_slug for w in output_workspaces):
        output_workspaces.insert(
            0,
            {
                "slug": personal_slug,
                "name": "اسناد شخصی من (Personal)",
                "scope": "personal",
                "folder_id": 0,
            },
        )

    # Store in thread-safe session cache
    session_acl_cache.set(
        token_key=session_token,
        user_id=uid,
        groups=groups,
        workspaces=output_workspaces,
        allowed_slugs=allowed_slugs,
    )
    sys.stderr.write(
        f"[SESSION ACL SYNC] Synchronized ACL cache for user '{uid}' ({len(allowed_slugs)} workspaces authorized, admin={is_admin}).\n"
    )
    return True, uid, groups, allowed_slugs, output_workspaces, 200, "OK"


class SafeAIBOXProxyHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def get_cors_headers(self) -> Dict[str, str]:
        origin = self.headers.get("Origin", "").strip()
        if not origin:
            return {}
        normalized = origin.rstrip("/").lower()
        if normalized in ALLOWED_ORIGINS:
            return {
                "Access-Control-Allow-Origin": origin,
                "Access-Control-Allow-Credentials": "true",
                "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS, PATCH, HEAD",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Requested-With, OCS-APIRequest, Accept",
                "Access-Control-Max-Age": "86400",
            }
        return {}

    def send_json_error(self, code: int, message: str, cors_headers: Optional[Dict[str, str]] = None):
        body = json.dumps({"success": False, "error": message}).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cors_headers:
            for k, v in cors_headers.items():
                self.send_header(k, v)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_OPTIONS(self):
        cors = self.get_cors_headers()
        self.send_response(204)
        for k, v in cors.items():
            self.send_header(k, v)
        self.send_header("Content-Length", "0")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

    def do_ALL(self, method: str):
        parsed_url = urllib.parse.urlsplit(self.path)
        path = parsed_url.path
        cors = self.get_cors_headers()

        # 1. Origin & CSRF Check for untrusted cross-origin requests
        origin = self.headers.get("Origin", "").strip()
        if origin:
            normalized_origin = origin.rstrip("/").lower()
            if normalized_origin not in ALLOWED_ORIGINS and method in ("POST", "PUT", "DELETE", "PATCH"):
                sys.stderr.write(f"[SECURITY BLOCKED] Untrusted cross-origin request from '{origin}' to '{path}'\n")
                self.send_json_error(403, f"Cross-origin request rejected: Origin '{origin}' is not authorized.", cors)
                return

        # 2. Check for Prohibited Routes
        for pat in PROHIBITED_PATH_PATTERNS:
            if pat.search(path):
                sys.stderr.write(f"[SECURITY BLOCKED] Prohibited ingestion endpoint: {path}\n")
                self.send_json_error(
                    403,
                    f"Disabled: Endpoint {path} is prohibited. Only supported text file uploads are permitted in AI Box.",
                    cors,
                )
                return

        # 3. Block Admin and System Routes unconditionally for ALL callers (including service key)
        for pat in ADMIN_SYSTEM_PATTERNS:
            if pat.search(path):
                sys.stderr.write(f"[SECURITY BLOCKED] Admin/System endpoint '{path}' blocked unconditionally.\n")
                self.send_json_error(403, "Access denied: Administrative and system APIs are restricted.", cors)
                return

        # 4. Identify Caller: Internal Service (Connector) vs Public User
        auth_header = self.headers.get("Authorization", "").strip()
        is_internal_service = bool(
            INTERNAL_SERVICE_KEY
            and len(INTERNAL_SERVICE_KEY) >= 16
            and auth_header == f"Bearer {INTERNAL_SERVICE_KEY}"
        )

        # Block Public Access to Developer Ingest Routes
        if not is_internal_service:
            for pat in INTERNAL_INGEST_PATTERNS:
                if pat.search(path):
                    sys.stderr.write(f"[SECURITY BLOCKED] Ingestion endpoint '{path}' denied to public request.\n")
                    self.send_json_error(
                        403,
                        "Access denied: Document ingestion and workspace management endpoints are restricted to internal services.",
                        cors,
                    )
                    return

        # 5. Handle Internal Service Requests (strictly limited to permitted operations)
        if is_internal_service:
            if not any(pat.search(path) for pat in ALLOWED_INTERNAL_PATTERNS):
                sys.stderr.write(f"[SECURITY BLOCKED] Endpoint '{path}' is not permitted for internal service key.\n")
                self.send_json_error(403, "Access denied: Operation not permitted for internal service key.", cors)
                return

            is_upload = any(pat.search(path) for pat in UPLOAD_PATH_PATTERNS) and method == "POST"
            content_length = int(self.headers.get("Content-Length", 0))
            content_type = self.headers.get("Content-Type", "")

            req_body = b""
            if content_length > 0:
                if is_upload and content_length > (MAX_FILE_SIZE + 65536):
                    sys.stderr.write(f"[REJECTED] Upload size {content_length} exceeds limit\n")
                    self.send_json_error(413, f"Payload Too Large: Upload size exceeds {MAX_FILE_SIZE} bytes.", cors)
                    return
                req_body = self.rfile.read(content_length)

            if is_upload and "multipart/form-data" in content_type:
                boundary = parse_multipart_boundary(content_type)
                if boundary:
                    ok, err = inspect_multipart_body(req_body, boundary)
                    if not ok:
                        sys.stderr.write(f"[UPLOAD REJECTED] {err}\n")
                        self.send_json_error(400, err, cors)
                        return
                    sys.stderr.write(f"[UPLOAD VALIDATED] Supported text file validation passed for {path}\n")

            self._forward_to_backend(method, req_body, cors, strip_auth=False)
            return

        # 6. Handle Public User Requests: Authentication & Access Control

        # Case 6E: Session Invalidation / Logout / Manual ACL Sync
        if SESSION_INVALIDATE_PATTERN.match(path):
            session_token = extract_session_token(dict(self.headers))
            if session_token:
                session_acl_cache.invalidate(session_token)
                sys.stderr.write(f"[SESSION INVALIDATED] Cache cleared for active session.\n")
            body = json.dumps({"success": True, "message": "Session invalidated and ACL cache cleared."}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for k, v in cors.items():
                self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            return

        if SESSION_SYNC_PATTERN.match(path):
            is_auth, uid, groups, allowed_slugs, archive_workspaces, err_code, err_msg = get_or_sync_session_acl(
                dict(self.headers), force_refresh=True
            )
            if not is_auth:
                self.send_json_error(err_code, err_msg, cors)
                return
            body = json.dumps({
                "success": True,
                "user_id": uid,
                "workspaces_count": len(archive_workspaces),
                "message": "Session ACL cache synchronized successfully.",
            }).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            for k, v in cors.items():
                self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()
            return

        # Case 6A: Workspace Specific Route (Chat, Stream-Chat, History, Info)
        ws_match = WORKSPACE_USER_PATH_PATTERN.match(path)
        if ws_match:
            slug = ws_match.group("slug")
            subpath = (ws_match.group("subpath") or "").rstrip("/").lower()

            # Retrieve session & authorized workspaces from session ACL cache (or sync once on initial session access)
            is_auth, uid, groups, allowed_slugs, archive_workspaces, err_code, err_msg = get_or_sync_session_acl(dict(self.headers))
            if not is_auth:
                sys.stderr.write(f"[AUTH REJECTED] Session verification failed for '{path}' (status {err_code})\n")
                self.send_json_error(err_code, err_msg, cors)
                return

            if slug not in allowed_slugs:
                if slug.startswith("user-") or slug.startswith("personal-"):
                    sys.stderr.write(
                        f"[ACCESS DENIED] User '{uid}' attempted to access unauthorized personal workspace '{slug}'.\n"
                    )
                    self.send_json_error(
                        403,
                        f"Access denied: Personal workspace '{slug}' does not belong to user '{uid}'.",
                        cors,
                    )
                    return
                sys.stderr.write(
                    f"[ACCESS DENIED] User '{uid}' is not authorized for workspace '{slug}'. Authorized: {allowed_slugs}\n"
                )
                self.send_json_error(
                    403,
                    f"Access denied: Workspace '{slug}' is not mapped to any authorized group.",
                    cors,
                )
                return

            sys.stderr.write(f"[ACCESS GRANTED] User '{uid}' authorized in real-time for workspace '{slug}'.\n")

            # Enforce strict Origin and CSRF custom header on user chat operations before forwarding
            is_chat_post = method == "POST" and subpath in ("/chat", "/stream-chat")
            if is_chat_post:
                origin = self.headers.get("Origin", "").strip()
                if not origin or origin.lower() == "null":
                    sys.stderr.write(f"[SECURITY BLOCKED] Missing or null Origin on chat endpoint '{path}'\n")
                    self.send_json_error(
                        403,
                        "Access denied: Valid Origin header is required for chat operations.",
                        cors,
                    )
                    return

                normalized_origin = origin.rstrip("/").lower()
                if normalized_origin not in ALLOWED_ORIGINS:
                    sys.stderr.write(f"[SECURITY BLOCKED] Unauthorized Origin '{origin}' on chat endpoint '{path}'\n")
                    self.send_json_error(
                        403,
                        f"Access denied: Origin '{origin}' is not authorized for chat operations.",
                        cors,
                    )
                    return

                archive_chat_header = self.headers.get("X-Archive-Chat", "").strip().lower()
                if archive_chat_header not in ("true", "1"):
                    sys.stderr.write(f"[SECURITY BLOCKED] Missing or invalid X-Archive-Chat header on '{path}'\n")
                    self.send_json_error(
                        403,
                        "Access denied: Missing or invalid X-Archive-Chat header.",
                        cors,
                    )
                    return

            # Read user request body if present
            content_length = int(self.headers.get("Content-Length", 0))
            req_body = self.rfile.read(content_length) if content_length > 0 else b""

            # Forward to backend, stripping Archive Cookie/Auth headers
            self._forward_to_backend(method, req_body, cors, strip_auth=True)
            return

        # Case 6B: Workspaces Listing for Public User (/api/v1/workspaces)
        if WORKSPACES_LIST_PATTERN.match(path) and method == "GET":
            # JIT Live Synchronization: Always query fresh Nextcloud ACL on workspaces listing
            is_auth, uid, groups, allowed_slugs, archive_workspaces, err_code, err_msg = get_or_sync_session_acl(
                dict(self.headers), force_refresh=True
            )
            if not is_auth:
                sys.stderr.write(f"[AUTH REJECTED] Session verification failed for workspaces list\n")
                self.send_json_error(err_code, err_msg, cors)
                return

            self._filter_and_send_workspaces_list(uid, groups, archive_workspaces, cors)
            return

        # Case 6C: Static Assets & UI Files
        if STATIC_ASSET_PATTERN.match(path) and method == "GET":
            self._forward_to_backend(method, b"", cors, strip_auth=True)
            return

        # Case 6D: Any other public request to unmapped endpoints -> Deny
        sys.stderr.write(f"[ACCESS BLOCKED] Public request to unmapped endpoint '{path}' rejected.\n")
        self.send_json_error(403, f"Access denied: Endpoint '{path}' is not permitted for public access.", cors)

    def _filter_and_send_workspaces_list(
        self,
        user_id: str,
        user_groups: List[str],
        archive_workspaces: List[Dict[str, Any]],
        cors: Dict[str, str],
    ) -> None:
        """Uses cached or synchronized Archive workspaces, auto-provisions them in the backend if missing, and returns the allowlisted list."""

        is_admin = (user_id == "admin" or "admin" in user_groups)

        # Fetch existing workspaces from active backend (AnythingLLM simulator or External AI Box)
        backend_workspaces = []
        try:
            _mod = sys.modules.get(__name__) or sys.modules.get("proxy")
            conn_fn = getattr(_mod, "create_backend_connection", create_backend_connection)
            conn = conn_fn(BACKEND_HOST, BACKEND_PORT, timeout=10.0)
            headers = {"Accept": "application/json"}
            if INTERNAL_SERVICE_KEY:
                headers["Authorization"] = f"Bearer {INTERNAL_SERVICE_KEY}"
            conn.request("GET", "/api/v1/workspaces", headers=headers)
            res = conn.getresponse()
            raw_body = res.read().decode("utf-8", errors="ignore")
            conn.close()
            if res.status == 200:
                backend_data = json.loads(raw_body)
                backend_workspaces = backend_data.get("workspaces", [])
        except Exception as e:
            sys.stderr.write(f"[GATEWAY] Could not query backend workspaces ({AI_BACKEND_PROVIDER}): {e}\n")

        backend_ws_map = {
            ws.get("slug"): ws
            for ws in backend_workspaces
            if isinstance(ws, dict) and ws.get("slug")
        }
        backend_slugs = set(backend_ws_map.keys())

        # Auto-provision any missing workspace in active backend
        output_workspaces = []
        for idx, ws in enumerate(archive_workspaces):
            if not isinstance(ws, dict):
                continue
            slug = ws.get("slug")
            name = ws.get("name") or slug
            if not slug:
                continue

            # Strict Guard: Enterprise_Archive is strictly restricted to administrator
            is_ea = (
                str(name).lower() == "enterprise_archive"
                or str(slug).lower() == "enterprise_archive"
                or "enterprise_archive" in str(slug).lower()
                or "enterprise_archive" in str(name).lower()
                or str(slug).endswith("-346")
            )
            if is_ea and not is_admin:
                continue

            if slug not in backend_slugs:
                try:
                    _mod = sys.modules.get(__name__) or sys.modules.get("proxy")
                    conn_fn = getattr(_mod, "create_backend_connection", create_backend_connection)
                    conn_create = conn_fn(BACKEND_HOST, BACKEND_PORT, timeout=10.0)
                    h_create = {"Content-Type": "application/json", "Accept": "application/json"}
                    if INTERNAL_SERVICE_KEY:
                        h_create["Authorization"] = f"Bearer {INTERNAL_SERVICE_KEY}"

                    # Contract adapter: AnythingLLM uses POST /api/v1/workspace/new with {"name": slug}
                    # External AI Box uses POST /api/v1/workspaces with {"slug": slug, "name": name}
                    if AI_BACKEND_PROVIDER in ("external", "external_ai_box", "remote"):
                        body_create = json.dumps({"slug": slug, "name": name}).encode("utf-8")
                        conn_create.request("POST", "/api/v1/workspaces", body=body_create, headers=h_create)
                    else:
                        body_create = json.dumps({"name": slug}).encode("utf-8")
                        conn_create.request("POST", "/api/v1/workspace/new", body=body_create, headers=h_create)

                    resp_create = conn_create.getresponse()
                    resp_create.read()
                    conn_create.close()
                    backend_slugs.add(slug)
                except Exception as e:
                    sys.stderr.write(f"[GATEWAY] Could not auto-create workspace '{slug}' ({AI_BACKEND_PROVIDER}): {e}\n")

            # Project cleanly to strict public fields: id, name, slug (ensures 100% compliance with security allowlist & frontend)
            b_ws = backend_ws_map.get(slug, {})
            ws_id = b_ws.get("id") or ws.get("id") or (idx + 1)
            output_workspaces.append({
                "id": ws_id,
                "name": name,
                "slug": slug,
            })

        body = json.dumps({"workspaces": output_workspaces}, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in cors.items():
            self.send_header(k, v)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def _forward_to_backend(
        self,
        method: str,
        req_body: bytes,
        cors: Dict[str, str],
        strip_auth: bool = False,
    ) -> None:
        """Forwards validated request to AI backend (AnythingLLM or External AI Box), streaming responses cleanly."""
        try:
            _mod = sys.modules.get(__name__) or sys.modules.get("proxy")
            conn_fn = getattr(_mod, "create_backend_connection", create_backend_connection)
            conn = conn_fn(BACKEND_HOST, BACKEND_PORT, timeout=300.0)

            fwd_headers = {}
            for k, v in self.headers.items():
                lower_k = k.lower()
                if lower_k in ("host", "connection"):
                    continue
                # Strip user's archive credentials to prevent leakage to LLM backend
                if strip_auth and lower_k in ("cookie", "authorization"):
                    continue
                fwd_headers[k] = v

            fwd_headers["Host"] = f"{BACKEND_HOST}:{BACKEND_PORT}"
            if strip_auth and INTERNAL_SERVICE_KEY:
                fwd_headers["Authorization"] = f"Bearer {INTERNAL_SERVICE_KEY}"

            fwd_path = self.path
            if "/chat" in fwd_path or "/stream-chat" in fwd_path:
                if fwd_path.startswith("/api/workspace/"):
                    fwd_path = "/api/v1/workspace/" + fwd_path[len("/api/workspace/"):]
                elif fwd_path.startswith("/workspace/"):
                    fwd_path = "/api/v1/workspace/" + fwd_path[len("/workspace/"):]

            conn.request(method, fwd_path, body=req_body if req_body else None, headers=fwd_headers)
            res = conn.getresponse()

            is_chunked = False
            has_content_length = False
            for k, v in res.getheaders():
                lower_k = k.lower()
                if lower_k == "transfer-encoding" and "chunked" in v.lower():
                    is_chunked = True
                if lower_k == "content-length":
                    has_content_length = True

            self.send_response(res.status, res.reason)
            for k, v in res.getheaders():
                lower_k = k.lower()
                if lower_k in ("connection", "server", "date"):
                    continue
                if lower_k == "transfer-encoding" and "chunked" in v.lower():
                    continue
                self.send_header(k, v)

            if is_chunked:
                self.send_header("Transfer-Encoding", "chunked")
            
            self.send_header("Connection", "close")
            self.close_connection = True

            for k, v in cors.items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.flush()

            while True:
                chunk = res.read(8192)
                if not chunk:
                    break
                if is_chunked:
                    self.wfile.write(f"{len(chunk):X}\r\n".encode("ascii") + chunk + b"\r\n")
                else:
                    self.wfile.write(chunk)
                self.wfile.flush()

            if is_chunked:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()

            conn.close()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            sys.stderr.write(f"[GATEWAY ERROR] Backend forward failed: {e}\n")
            try:
                self.send_json_error(502, f"Bad Gateway: LLM backend error ({e}).", cors)
            except Exception:
                pass

    def log_message(self, format: str, *args):
        sanitized_args = []
        for a in args:
            if isinstance(a, str):
                sanitized_args.append(re.sub(r"\?[^\s]*", "", a))
            else:
                sanitized_args.append(a)
        sys.stderr.write("%s - - [%s] %s\n" % (self.client_address[0], self.log_date_time_string(), format % tuple(sanitized_args)))

    def do_GET(self): self.do_ALL("GET")
    def do_POST(self): self.do_ALL("POST")
    def do_PUT(self): self.do_ALL("PUT")
    def do_DELETE(self): self.do_ALL("DELETE")
    def do_HEAD(self): self.do_ALL("HEAD")
    def do_PATCH(self): self.do_ALL("PATCH")


def main():
    server = http.server.ThreadingHTTPServer(("0.0.0.0", GATEWAY_PORT), SafeAIBOXProxyHandler)
    sys.stderr.write(f"Starting Safe AI Box Proxy Gateway on 0.0.0.0:{GATEWAY_PORT}\n")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
