# -*- coding: utf-8 -*-
"""Destination Adapter for AI Box: Secure structured naming and idempotency pre-checks."""

import json
import logging
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple, Protocol

logger = logging.getLogger("AIBox.DestinationAdapter")

import os
from abc import ABC, abstractmethod

# Plug-and-Play AI Backend Engine Configuration
AI_BACKEND_PROVIDER = os.environ.get("AI_BACKEND_PROVIDER", "anythingllm").lower().strip()
AI_BACKEND_PROTOCOL = os.environ.get("AI_BACKEND_PROTOCOL", "http").lower().strip()
BACKEND_HOST = os.environ.get("BACKEND_HOST", "127.0.0.1").strip()
BACKEND_PORT = int(os.environ.get("BACKEND_PORT", "3001"))
INTERNAL_SERVICE_KEY = (
    os.environ.get("INTERNAL_SERVICE_KEY", "").strip()
    or os.environ.get("DESTINATION_API_KEY", "").strip()
)

try:
    from gid_mapper import GIDWorkspaceMapper
except ImportError:
    try:
        from connector.gid_mapper import GIDWorkspaceMapper
    except ImportError:
        GIDWorkspaceMapper = None

SUPPORTED_TEXT_EXTENSIONS = (
    ".txt", ".md", ".log", ".csv", ".json", ".xml", ".yaml", ".yml"
)

class WorkspaceResolutionError(Exception):
    """Raised when workspace resolution fails or receives invalid response from destination."""
    pass

class AmbiguousWorkspaceError(WorkspaceResolutionError):
    """Raised when multiple workspaces match the requested group display name."""
    pass

class DeliveryStatus(Enum):
    SUCCESS = "SUCCESS"
    ALREADY_PROCESSED = "ALREADY_PROCESSED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    RETRYABLE_FAILURE = "RETRYABLE_FAILURE"
    FATAL_FAILURE = "FATAL_FAILURE"

@dataclass(frozen=True)
class DeliveryContract:
    source_file_id: str
    file_name: str
    mime_type: str
    content_hash: str
    file_payload: bytes
    workspace_slug: Optional[str] = None

@dataclass(frozen=True)
class DeliveryResult:
    status: DeliveryStatus
    message: str
    doc_location: Optional[str] = None
    target_filename: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None
    workspace_slug: Optional[str] = None

class DestinationAdapterProtocol(Protocol):
    """Abstract protocol for destination adapters to ensure core decoupling."""
    def deliver(self, contract: DeliveryContract) -> DeliveryResult: ...
    def check_existing_document(self, source_file_id: str, content_hash: str) -> Tuple[Optional[DeliveryStatus], Optional[str]]: ...
    def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool: ...

def build_structured_filename(source_file_id: str, content_hash: str, original_filename: str) -> str:
    """Builds a secure structured filename preserving Persian/Unicode while strictly capping UTF-8 byte length.
    
    Pattern: arch_{source_file_id}_{full_sha256}_{safe_filename}.txt
    Max total UTF-8 bytes capped to 180 to guarantee ample headroom for filesystem limits and AnythingLLM suffixes.
    """
    clean_hash = content_hash.lower().strip()
    if len(clean_hash) != 64 or not all(c in "0123456789abcdef" for c in clean_hash):
        raise ValueError(f"Invalid SHA-256 hash: '{content_hash}'. Must be 64-character hex string.")

    # Strip directory traversal and dangerous characters
    base_name = original_filename.replace("\\", "/").split("/")[-1]
    # Remove control characters and NUL
    base_name = re.sub(r"[\x00-\x1f\x7f]", "", base_name)
    # Replace dangerous filesystem chars (< > : " / \ | ? *) with underscore
    base_name = re.sub(r'[<>:"/\\|?*]', "_", base_name)
    
    # Determine and preserve supported file extension
    suffix = ".txt"
    lower_base = base_name.lower()
    for ext in SUPPORTED_TEXT_EXTENSIONS:
        if lower_base.endswith(ext):
            suffix = ext
            base_name = base_name[:-len(ext)]
            break

    # Strip leading/trailing dots and spaces
    base_name = base_name.strip(" .")
    if not base_name:
        base_name = "document"

    prefix = f"arch_{source_file_id}_{clean_hash}_"
    prefix_bytes = len(prefix.encode("utf-8"))
    suffix_bytes = len(suffix.encode("utf-8"))
    max_total_bytes = 180
    max_name_bytes = max(10, max_total_bytes - prefix_bytes - suffix_bytes)

    # Safely truncate base_name by UTF-8 bytes without slicing characters mid-byte
    encoded_base = base_name.encode("utf-8")
    if len(encoded_base) > max_name_bytes:
        truncated = ""
        for ch in base_name:
            if len((truncated + ch).encode("utf-8")) <= max_name_bytes:
                truncated += ch
            else:
                break
        base_name = truncated.rstrip(" ._")
        if not base_name:
            base_name = "doc"

    return f"{prefix}{base_name}{suffix}"

class BaseAIBackendEngine(ABC):
    """Abstract interface for plug-and-play AI Box backend engines."""

    @abstractmethod
    def ensure_workspace_exists(self, slug: str, name: Optional[str] = None) -> bool:
        pass

    @abstractmethod
    def inspect_workspace_documents(self, slug: str) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def deliver_document(self, contract: DeliveryContract, target_slug: str, target_filename: str) -> DeliveryResult:
        pass

    @abstractmethod
    def remove_document(self, doc_location: str, workspace_slug: str) -> bool:
        pass


class AnythingLLMEngineAdapter(BaseAIBackendEngine):
    """Simulator backend engine communicating with AnythingLLM + LanceDB."""

    def __init__(self, adapter: "LocalAiBoxAdapter"):
        self.adapter = adapter

    def ensure_workspace_exists(self, slug: str, name: Optional[str] = None) -> bool:
        return self.adapter.ensure_workspace_exists(slug, name)

    def inspect_workspace_documents(self, slug: str) -> List[Dict[str, Any]]:
        return self.adapter.inspect_workspace_documents(slug)

    def deliver_document(self, contract: DeliveryContract, target_slug: str, target_filename: str) -> DeliveryResult:
        return self.adapter._anythingllm_deliver(contract, target_slug, target_filename)

    def remove_document(self, doc_location: str, workspace_slug: str) -> bool:
        return self.adapter.remove_document_from_workspace(doc_location, workspace_slug)


class ExternalAIBoxEngineAdapter(BaseAIBackendEngine):
    """Official plug-and-play adapter for External AI Box REST API specification."""

    def __init__(self, adapter: "LocalAiBoxAdapter"):
        self.adapter = adapter

    def ensure_workspace_exists(self, slug: str, name: Optional[str] = None) -> bool:
        """Contract 1: Workspace Auto-Provisioning.
        Target: POST /api/v1/workspaces with payload {"slug": slug, "name": name}.
        """
        target_slug = str(slug).strip()
        url = f"{self.adapter.gateway_url}/api/v1/workspaces"
        payload = json.dumps({"slug": target_slug, "name": name or target_slug}).encode("utf-8")
        headers = self.adapter._build_headers({"Content-Type": "application/json"})
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.adapter.timeout) as resp:
                return resp.status in (200, 201)
        except urllib.error.HTTPError as e:
            if e.code == 409:
                return True
            logger.error("External AI Box failed to provision workspace '%s' (HTTP %d)", target_slug, e.code)
            return False
        except Exception as e:
            logger.error("External AI Box connection error during workspace provisioning '%s': %s", target_slug, e)
            return False

    def inspect_workspace_documents(self, slug: str) -> List[Dict[str, Any]]:
        """Contract 2A: Query workspace documents.
        Target: GET /api/v1/workspaces/:slug/documents.
        """
        target_slug = str(slug).strip()
        url = f"{self.adapter.gateway_url}/api/v1/workspaces/{target_slug}/documents"
        headers = self.adapter._build_headers({"Accept": "application/json"})
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.adapter.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                docs = data.get("documents", []) if isinstance(data, dict) else []
                return [
                    {
                        "filename": d.get("filename") or d.get("name") or "",
                        "docpath": d.get("id") or d.get("location") or d.get("filename") or "",
                    }
                    for d in docs if isinstance(d, dict)
                ]
        except Exception as e:
            logger.debug("External AI Box query documents returned error for '%s': %s", target_slug, e)
            return []

    def deliver_document(self, contract: DeliveryContract, target_slug: str, target_filename: str) -> DeliveryResult:
        """Contract 2B: Document Ingestion & Embedding.
        Target: POST /api/v1/workspaces/:slug/documents with multipart payload.
        """
        boundary = "----AIBOXExternalBoundary" + contract.content_hash[:12]
        crlf = b"\r\n"
        body_parts = []
        body_parts.append(f"--{boundary}".encode("latin1") + crlf)
        body_parts.append(
            f'Content-Disposition: form-data; name="file"; filename="{target_filename}"'.encode("utf-8") + crlf
        )
        body_parts.append(b"Content-Type: text/plain; charset=utf-8" + crlf + crlf)
        body_parts.append(contract.file_payload + crlf)
        body_parts.append(f"--{boundary}--".encode("latin1") + crlf)
        multipart_body = b"".join(body_parts)

        url = f"{self.adapter.gateway_url}/api/v1/workspaces/{target_slug}/documents"
        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(multipart_body)),
            "Accept": "application/json",
            "X-Source-File-Id": str(contract.source_file_id),
            "X-Content-SHA256": str(contract.content_hash),
        }
        if self.adapter.api_key:
            headers["Authorization"] = f"Bearer {self.adapter.api_key}"

        req = urllib.request.Request(url, data=multipart_body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.adapter.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                doc_id = data.get("document_id") or data.get("id") or target_filename
                return DeliveryResult(
                    status=DeliveryStatus.SUCCESS,
                    message="Document successfully ingested and indexed in External AI Box.",
                    doc_location=str(doc_id),
                    target_filename=target_filename,
                    raw_response=data,
                    workspace_slug=target_slug,
                )
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="ignore")
            if e.code in (400, 403, 413):
                return DeliveryResult(
                    status=DeliveryStatus.FATAL_FAILURE,
                    message=f"External AI Box rejected payload (HTTP {e.code}): {err_body}",
                    target_filename=target_filename,
                )
            return DeliveryResult(
                status=DeliveryStatus.RETRYABLE_FAILURE,
                message=f"External AI Box server error (HTTP {e.code}): {err_body}",
                target_filename=target_filename,
            )
        except Exception as e:
            return DeliveryResult(
                status=DeliveryStatus.RETRYABLE_FAILURE,
                message=f"Network error connecting to External AI Box: {e}",
                target_filename=target_filename,
            )

    def remove_document(self, doc_location: str, workspace_slug: str) -> bool:
        """Contract 3: Document Vector Deletion.
        Target: DELETE /api/v1/workspaces/:slug/documents/:document_id.
        """
        target_slug = str(workspace_slug).strip()
        encoded_doc_id = urllib.parse.quote(str(doc_location).strip(), safe="")
        url = f"{self.adapter.gateway_url}/api/v1/workspaces/{target_slug}/documents/{encoded_doc_id}"
        headers = self.adapter._build_headers({"Accept": "application/json"})
        req = urllib.request.Request(url, headers=headers, method="DELETE")
        try:
            with urllib.request.urlopen(req, timeout=self.adapter.timeout) as resp:
                return resp.status in (200, 204)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return True
            logger.error("External AI Box failed to delete document '%s' (HTTP %d)", doc_location, e.code)
            return False
        except Exception as e:
            logger.error("External AI Box connection error during document deletion: %s", e)
            return False


class LocalAiBoxAdapter:
    """Delivers documents to AI Box gateway with modular plug-and-play backend support."""

    def __init__(
        self,
        gateway_url: str = "http://127.0.0.1:3001",
        workspace_slug: str = "ai-box-test",
        api_key: Optional[str] = None,
        timeout: int = 60,
        gid_mapper: Optional[Any] = None,
        backend_provider: Optional[str] = None,
    ):
        self.gateway_url = gateway_url.rstrip("/")
        self.workspace_slug = workspace_slug
        self.api_key = api_key or INTERNAL_SERVICE_KEY or None
        self.timeout = timeout
        self.backend_provider = (backend_provider or AI_BACKEND_PROVIDER).lower().strip()
        if gid_mapper is not None:
            self.gid_mapper = gid_mapper
        elif GIDWorkspaceMapper is not None:
            self.gid_mapper = GIDWorkspaceMapper()
        else:
            self.gid_mapper = None

        # Select backend engine based on provider configuration
        if self.backend_provider in ("external", "external_ai_box", "remote"):
            self.engine: BaseAIBackendEngine = ExternalAIBoxEngineAdapter(self)
        else:
            self.engine = AnythingLLMEngineAdapter(self)

    def _ensure_authenticated(self, operation: str) -> None:
        """Enforces that a non-empty API key is configured before issuing requests to protected Developer API endpoints."""
        if not self.api_key or not self.api_key.strip():
            raise WorkspaceResolutionError(
                f"Destination API key is missing or empty. Safe halt before issuing request to {operation}."
            )

    def _build_headers(self, additional: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
        }
        if self.api_key and self.api_key.strip():
            headers["Authorization"] = f"Bearer {self.api_key.strip()}"
        if additional:
            headers.update(additional)
        return headers

    def list_workspaces(self) -> List[Dict[str, Any]]:
        """Queries GET /api/v1/workspaces to list all workspaces in AnythingLLM Developer API."""
        self._ensure_authenticated("GET /api/v1/workspaces")
        url = f"{self.gateway_url}/api/v1/workspaces"
        req = urllib.request.Request(url, headers=self._build_headers())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (401, 403):
                raise WorkspaceResolutionError(f"Authentication failed on /api/v1/workspaces (HTTP {e.code}): Access denied.")
            if 400 <= e.code < 500:
                raise WorkspaceResolutionError(f"Client error fetching workspaces (HTTP {e.code}): {e.reason}")
            # 5xx server errors bubble up as HTTPError (transient)
            raise
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            raise ConnectionError(f"Failed to fetch workspaces from AI Box Gateway: {e}")

        if not isinstance(data, dict) or "workspaces" not in data:
            raise WorkspaceResolutionError(
                "Invalid or incomplete response from /api/v1/workspaces: missing 'workspaces' root key"
            )

        ws_list = data.get("workspaces")
        if not isinstance(ws_list, list):
            raise WorkspaceResolutionError(
                "Unexpected response structure from /api/v1/workspaces: 'workspaces' is not a list"
            )
        return ws_list

    def create_workspace(self, name: str) -> Dict[str, Any]:
        """Issues POST /api/v1/workspace/new to create a new workspace with specified display name."""
        self._ensure_authenticated("POST /api/v1/workspace/new")
        url = f"{self.gateway_url}/api/v1/workspace/new"
        payload = json.dumps({"name": name}).encode("utf-8")
        headers = self._build_headers({"Content-Type": "application/json"})
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if 400 <= e.code < 500:
                err_body = e.read().decode("utf-8", errors="ignore")
                raise WorkspaceResolutionError(f"AnythingLLM rejected workspace creation for '{name}' (HTTP {e.code}): {err_body}")
            # 5xx server errors bubble up as HTTPError (transient)
            raise
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            raise ConnectionError(f"Failed to create workspace '{name}' on AI Box Gateway: {e}")

        if not isinstance(data, dict):
            raise WorkspaceResolutionError(f"Invalid non-dict response from /api/v1/workspace/new for '{name}'")

        ws = data.get("workspace")
        if not ws or not isinstance(ws, dict):
            err = data.get("error") or data.get("message") or "Unknown error creating workspace"
            raise WorkspaceResolutionError(f"AnythingLLM rejected workspace creation for '{name}': {err}")

        slug = ws.get("slug")
        if not slug or not str(slug).strip():
            raise WorkspaceResolutionError(f"Created workspace for '{name}' but response contains missing or empty slug")

        return ws

    def ensure_workspace_exists(self, workspace_slug: str, display_name: Optional[str] = None) -> str:
        """Verifies if workspace_slug exists; auto-provisions it via active backend engine if missing."""
        slug_clean = workspace_slug.strip()
        if isinstance(getattr(self, "engine", None), ExternalAIBoxEngineAdapter):
            self.engine.ensure_workspace_exists(slug_clean, display_name)
            return slug_clean
        try:
            existing = self.list_workspaces()
            for ws in existing:
                if isinstance(ws, dict) and ws.get("slug") == slug_clean:
                    return slug_clean
        except Exception as e:
            logger.warning("Could not query existing workspaces for '%s': %s", slug_clean, e)

        # Workspace not found -> Auto-provision via official API
        name_to_use = (display_name or slug_clean).strip()
        logger.info("Workspace '%s' not found in AnythingLLM. Auto-provisioning with name '%s'...", slug_clean, name_to_use)
        try:
            created = self.create_workspace(name_to_use)
            created_slug = created.get("slug")
            return str(created_slug or slug_clean)
        except Exception as e:
            logger.error("Failed to auto-provision workspace '%s': %s", slug_clean, e)
            return slug_clean

    def resolve_or_create_group_workspace(self, group_display_name: str) -> str:
        """Resolves the real technical slug for a group's workspace, creating it via official API if missing.
        
        Rules:
        - Group display name must be non-empty string.
        - Matches exact group display name against existing workspaces.
        - Halts with AmbiguousWorkspaceError if multiple workspaces match.
        - If missing, creates the workspace via POST /api/v1/workspace/new and takes the slug from API response.
        - If response structure is invalid, halts with WorkspaceResolutionError and never creates a duplicate.
        - Never guesses the slug or falls back to a public/default workspace.
        """
        if not group_display_name or not isinstance(group_display_name, str):
            raise ValueError("Group display name must be a non-empty string.")
        clean_name = group_display_name.strip()
        if not clean_name:
            raise ValueError("Group display name cannot be blank or whitespace.")

        # Fetch current workspaces list from destination
        existing = self.list_workspaces()

        # 1. Check persistent GID -> workspace_slug mapping first if available
        if self.gid_mapper:
            mapped_slug = self.gid_mapper.get_slug(clean_name)
            if mapped_slug:
                matched = [ws for ws in existing if isinstance(ws, dict) and ws.get("slug") == mapped_slug]
                if matched:
                    return mapped_slug
                logger.warning(
                    "Workspace for GID '%s' was mapped to '%s' but no longer exists in AnythingLLM. Re-resolving.",
                    clean_name, mapped_slug
                )
        matches = [
            ws for ws in existing
            if isinstance(ws, dict) and ws.get("name") == clean_name
        ]

        if len(matches) > 1:
            raise AmbiguousWorkspaceError(
                f"Multiple workspaces ({len(matches)}) found matching group display name '{clean_name}'. Halting to prevent misrouting."
            )

        if len(matches) == 1:
            slug = matches[0].get("slug")
            if not slug:
                raise WorkspaceResolutionError(
                    f"Workspace with name '{clean_name}' exists but has missing or empty slug: {matches[0]}"
                )
            slug_str = str(slug)
            if self.gid_mapper:
                self.gid_mapper.set_mapping(clean_name, slug_str, clean_name)
            return slug_str

        # Create new workspace via official API
        created_ws = self.create_workspace(clean_name)
        new_slug = created_ws.get("slug")
        if not new_slug:
            raise WorkspaceResolutionError(
                f"Created workspace for '{clean_name}' but AnythingLLM returned no slug in response: {created_ws}"
            )
        slug_str = str(new_slug)
        if self.gid_mapper:
            self.gid_mapper.set_mapping(clean_name, slug_str, clean_name)
        return slug_str

    def inspect_workspace_documents(self, workspace_slug: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries GET /api/workspace/:slug to list currently attached documents."""
        target_slug = (workspace_slug or self.workspace_slug).strip()
        url = f"{self.gateway_url}/api/workspace/{target_slug}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            raise ConnectionError(f"Failed to inspect workspace documents: {e}")

        ws = data.get("workspace")
        if isinstance(ws, list):
            ws = ws[0] if ws else {}
        elif not isinstance(ws, dict):
            ws = {}
        return ws.get("documents", [])

    def check_existing_document(self, source_file_id: str, content_hash: str, workspace_slug: Optional[str] = None) -> Tuple[Optional[DeliveryStatus], Optional[str]]:
        """Scans workspace documents for matching structured filename prefix arch_{fileId}_{hash}_."""
        target_slug = (workspace_slug or self.workspace_slug).strip()
        try:
            docs = self.inspect_workspace_documents(target_slug)
        except ConnectionError:
            # If inspection fails due to transient connection, let caller handle retry
            return None, None

        clean_hash = content_hash.lower().strip()
        prefix = f"arch_{source_file_id}_"
        expected_exact_prefix = f"arch_{source_file_id}_{clean_hash}_"

        for doc in docs:
            filename = doc.get("filename", "") or ""
            docpath = doc.get("docpath", "") or ""
            name_to_check = docpath.split("/")[-1] if docpath else filename

            if name_to_check.startswith(expected_exact_prefix):
                return DeliveryStatus.ALREADY_PROCESSED, docpath
            if name_to_check.startswith(prefix):
                # Document exists with same source ID but different content hash!
                # Mark NEEDS_REVIEW to halt blind overwrite
                return DeliveryStatus.NEEDS_REVIEW, docpath

        return None, None

    def deliver(self, contract: DeliveryContract) -> DeliveryResult:
        """Idempotently delivers and embeds the contract document."""
        target_slug = (contract.workspace_slug or self.workspace_slug).strip()
        self.ensure_workspace_exists(target_slug)

        # 1. Reconciliation Pre-check: Avoid duplicate embedding
        existing_status, existing_docpath = self.check_existing_document(
            contract.source_file_id, contract.content_hash, workspace_slug=target_slug
        )
        if existing_status == DeliveryStatus.ALREADY_PROCESSED:
            return DeliveryResult(
                status=DeliveryStatus.ALREADY_PROCESSED,
                message=f"Document from source ID {contract.source_file_id} with matching hash is already indexed in workspace.",
                doc_location=existing_docpath,
                target_filename=existing_docpath.split("/")[-1] if existing_docpath else None,
                workspace_slug=target_slug,
            )
        if existing_status == DeliveryStatus.NEEDS_REVIEW:
            return DeliveryResult(
                status=DeliveryStatus.NEEDS_REVIEW,
                message=f"Document from source ID {contract.source_file_id} exists with a different hash or ambiguous state ({existing_docpath}). Auto-resend halted.",
                doc_location=existing_docpath,
                workspace_slug=target_slug,
            )

        # 2. Build safe structured filename with full SHA-256 and byte-length bounds
        target_filename = build_structured_filename(
            contract.source_file_id, contract.content_hash, contract.file_name
        )

        # Delegate to External AI Box if active provider
        if isinstance(getattr(self, "engine", None), ExternalAIBoxEngineAdapter):
            return self.engine.deliver_document(contract, target_slug, target_filename)

        # 3. Construct multipart/form-data payload with field name "file"
        boundary = "----AIBOXConnectorBoundary" + contract.content_hash[:12]
        crlf = b"\r\n"
        body_parts = []
        body_parts.append(f"--{boundary}".encode("latin1") + crlf)
        body_parts.append(
            f'Content-Disposition: form-data; name="file"; filename="{target_filename}"'.encode("utf-8") + crlf
        )
        body_parts.append(b"Content-Type: text/plain; charset=utf-8" + crlf + crlf)
        body_parts.append(contract.file_payload + crlf)
        body_parts.append(f"--{boundary}--".encode("latin1") + crlf)
        multipart_body = b"".join(body_parts)

        # 4. Issue POST request to /api/workspace/:slug/upload-and-embed
        upload_url = f"{self.gateway_url}/api/workspace/{target_slug}/upload-and-embed"
        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Content-Length": str(len(multipart_body)),
            "Accept": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        req = urllib.request.Request(upload_url, data=multipart_body, headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp_bytes = resp.read()
                try:
                    resp_json = json.loads(resp_bytes.decode("utf-8"))
                except json.JSONDecodeError:
                    resp_json = {}

                # Strict Verification: HTTP 200 alone is NOT enough; body.success must be true!
                if resp_json.get("success") is True and resp_json.get("document"):
                    doc_loc = resp_json["document"].get("location")
                    return DeliveryResult(
                        status=DeliveryStatus.SUCCESS,
                        message="File accepted and successfully indexed in LanceDB.",
                        doc_location=doc_loc,
                        target_filename=target_filename,
                        raw_response=resp_json,
                        workspace_slug=target_slug,
                    )
                else:
                    err_msg = resp_json.get("error") or "Unknown embedding worker failure"
                    return DeliveryResult(
                        status=DeliveryStatus.RETRYABLE_FAILURE,
                        message=f"AnythingLLM rejected embedding: {err_msg}",
                        target_filename=target_filename,
                        raw_response=resp_json,
                    )
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="ignore")
            # 4xx client errors (e.g. 400 validation, 403 prohibited, 413 oversized) are fatal
            if e.code in (400, 403, 413):
                return DeliveryResult(
                    status=DeliveryStatus.FATAL_FAILURE,
                    message=f"Gateway/Server rejected payload (HTTP {e.code}): {err_body}",
                    target_filename=target_filename,
                )
            # 5xx server errors are transient/retryable
            return DeliveryResult(
                status=DeliveryStatus.RETRYABLE_FAILURE,
                message=f"Destination server error (HTTP {e.code}): {err_body}",
                target_filename=target_filename,
            )
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            return DeliveryResult(
                status=DeliveryStatus.RETRYABLE_FAILURE,
                message=f"Network error connecting to AI Box Gateway: {e}",
                target_filename=target_filename,
            )

    def remove_document_from_workspace(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
        """Removes a specific document and purges its vector embeddings from a workspace via official API.

        API Route: POST /api/v1/workspace/:slug/update-embeddings
        Contract: Body contains {"adds": [], "deletes": [doc_location]}.
        Strictly limited to the specified workspace and document identifier.
        Validates response strictly: HTTP 200 alone is NOT enough; workspace must be a valid non-null dict.
        """
        if not doc_location or not str(doc_location).strip():
            logger.warning("remove_document_from_workspace called with empty doc_location.")
            return False

        target_slug = (workspace_slug or self.workspace_slug).strip()
        if isinstance(getattr(self, "engine", None), ExternalAIBoxEngineAdapter):
            return self.engine.remove_document(doc_location, target_slug)
        url = f"{self.gateway_url}/api/v1/workspace/{target_slug}/update-embeddings"
        payload = json.dumps({"adds": [], "deletes": [str(doc_location).strip()]}).encode("utf-8")
        headers = self._build_headers({"Content-Type": "application/json"})
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp_bytes = resp.read()
                try:
                    data = json.loads(resp_bytes.decode("utf-8"))
                except json.JSONDecodeError:
                    logger.error(
                        "Invalid non-JSON response from workspace update-embeddings for '%s' (status %d)",
                        target_slug, resp.status
                    )
                    return False

                if not isinstance(data, dict):
                    logger.error(
                        "Unexpected response structure (not dict) from update-embeddings for '%s'",
                        target_slug
                    )
                    return False

                # Strict Contract Validation for AnythingLLM:
                # 1. Any explicit error message indicates failure
                if data.get("error"):
                    logger.error(
                        "AnythingLLM update-embeddings returned error for workspace '%s': %s",
                        target_slug, data.get("error")
                    )
                    return False

                # 2. Workspace must be present and must be a valid dictionary
                ws = data.get("workspace")
                if not isinstance(ws, dict):
                    err_msg = data.get("message") or "Missing or null workspace in response"
                    logger.error(
                        "AnythingLLM update-embeddings failed for workspace '%s': %s",
                        target_slug, err_msg
                    )
                    return False

                # 3. Workspace dictionary must contain valid identifier (slug or id)
                if ws.get("slug") or ws.get("id"):
                    return True

                logger.error(
                    "Invalid workspace object returned from update-embeddings for '%s': %s",
                    target_slug, ws
                )
                return False
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", errors="ignore")
            logger.error(
                "Failed to remove document '%s' from workspace '%s' (HTTP %d): %s",
                doc_location, target_slug, e.code, err_body
            )
            return False
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            logger.error(
                "Connection error removing document '%s' from workspace '%s': %s",
                doc_location, target_slug, e
            )
            return False

    def remove_document(self, doc_location: str, workspace_slug: Optional[str] = None) -> bool:
        """Alias for remove_document_from_workspace."""
        return self.remove_document_from_workspace(doc_location, workspace_slug)

