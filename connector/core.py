# -*- coding: utf-8 -*-
"""Core Transfer Engine: Destination-agnostic orchestration of file pull and delivery."""

import fcntl
import os
import sys
from typing import Dict, Any, Optional, List
from config import ConnectorConfig
from source_webdav import WebDAVFileMetadata, validate_text_file, compute_sha256, WebDAVClient, WebDAVError
from destination_adapter import DeliveryContract, DeliveryStatus, DestinationAdapterProtocol
from state_store import StateStore

class ConcurrencyError(Exception):
    """Raised when another instance of the connector is already running."""
    pass

class ConnectorLock:
    """Process-level lock using fcntl.flock to prevent overlapping connector sync runs."""

    def __init__(self, lock_file_path: Optional[str] = None):
        if not lock_file_path:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            lock_file_path = os.path.join(base_dir, "connector.lock")
        self.lock_file_path = lock_file_path
        self._fd: Optional[int] = None

    def acquire(self) -> None:
        self._fd = os.open(self.lock_file_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
            os.close(self._fd)
            self._fd = None
            raise ConcurrencyError(f"Another connector process is currently running (lock held at '{self.lock_file_path}').")

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

class CoreTransferEngine:
    """Orchestrates file discovery, validation, state tracking, and pluggable delivery."""

    def __init__(
        self,
        config: ConnectorConfig,
        state_store: StateStore,
        destination_adapter: DestinationAdapterProtocol,
        source_client: Optional[Any] = None,
    ):
        self.config = config
        self.state_store = state_store
        self.adapter = destination_adapter
        self.source = source_client

    def process_file_payload(
        self, source_file_id: str, filename: str, content: bytes
    ) -> Dict[str, Any]:
        """Validates payload, enforces contract, and delegates delivery."""
        # 1. Content Pre-Validation (strict .txt, size, encoding)
        val = validate_text_file(filename, content, self.config.max_file_size)
        if not val.is_valid:
            self.state_store.update_record(
                source_file_id=source_file_id,
                filename=filename,
                content_hash="",
                status=DeliveryStatus.FATAL_FAILURE.value,
                error_message=val.error_message,
            )
            return {
                "file_id": source_file_id,
                "status": DeliveryStatus.FATAL_FAILURE.value,
                "message": val.error_message,
            }

        # 2. Compute SHA-256 Digest
        content_hash = compute_sha256(content)

        # 3. Check StateStore: If already successfully synced with exact same hash, skip
        existing_record = self.state_store.get(source_file_id)
        if existing_record:
            if existing_record.status in (DeliveryStatus.SUCCESS.value, DeliveryStatus.ALREADY_PROCESSED.value):
                if existing_record.content_hash == content_hash:
                    return {
                        "file_id": source_file_id,
                        "status": DeliveryStatus.ALREADY_PROCESSED.value,
                        "message": "File already synced according to local state.",
                    }
                else:
                    # Version conflict: source ID exists with different hash!
                    msg = f"Version conflict: Source ID {source_file_id} content changed. Auto-resend halted."
                    self.state_store.update_record(
                        source_file_id=source_file_id,
                        filename=filename,
                        content_hash=content_hash,
                        status=DeliveryStatus.NEEDS_REVIEW.value,
                        error_message=msg,
                    )
                    return {
                        "file_id": source_file_id,
                        "status": DeliveryStatus.NEEDS_REVIEW.value,
                        "message": msg,
                    }
            elif existing_record.status == DeliveryStatus.NEEDS_REVIEW.value:
                return {
                    "file_id": source_file_id,
                    "status": DeliveryStatus.NEEDS_REVIEW.value,
                    "message": "File is flagged as NEEDS_REVIEW. Auto-resend halted.",
                }

        # 4. Form Neutral Delivery Contract
        contract = DeliveryContract(
            source_file_id=str(source_file_id),
            file_name=filename,
            mime_type="text/plain",
            content_hash=content_hash,
            file_payload=content,
        )

        # 5. Handover to Destination Adapter
        result = self.adapter.deliver(contract)

        # 6. Record Status in StateStore
        is_retry = (result.status == DeliveryStatus.RETRYABLE_FAILURE)
        self.state_store.update_record(
            source_file_id=source_file_id,
            filename=filename,
            content_hash=content_hash,
            status=result.status.value,
            target_filename=result.target_filename,
            doc_location=result.doc_location,
            error_message=result.message if result.status != DeliveryStatus.SUCCESS else None,
            increment_retry=is_retry,
        )

        return {
            "file_id": source_file_id,
            "status": result.status.value,
            "message": result.message,
            "doc_location": result.doc_location,
        }

    def sync_folder(self, lock: Optional[ConnectorLock] = None) -> Dict[str, Any]:
        """Discovers files in configured WebDAV folder and synchronizes them under process lock."""
        if not self.source:
            raise ValueError("No WebDAV source client configured for folder sync.")

        stats = {
            "discovered": 0,
            "synced": 0,
            "skipped": 0,
            "needs_review": 0,
            "errors": 0,
        }

        # Acquire lock to prevent overlapping runs
        active_lock = lock or ConnectorLock()
        with active_lock:
            files_meta: List[WebDAVFileMetadata] = self.source.list_folder(self.config.webdav_folder)
            stats["discovered"] = len(files_meta)

            for meta in files_meta:
                # Pre-check StateStore before downloading content
                existing = self.state_store.get(meta.file_id)
                if existing and existing.status in (DeliveryStatus.SUCCESS.value, DeliveryStatus.ALREADY_PROCESSED.value):
                    # If file has not changed (same size and valid state), skip download
                    if existing.target_filename:
                        stats["skipped"] += 1
                        continue
                elif existing and existing.status == DeliveryStatus.NEEDS_REVIEW.value:
                    stats["needs_review"] += 1
                    continue

                try:
                    content = self.source.download_file(meta.href, max_bytes=self.config.max_file_size)
                    res = self.process_file_payload(meta.file_id, meta.name, content)
                    status = res.get("status")
                    if status == DeliveryStatus.SUCCESS.value:
                        stats["synced"] += 1
                    elif status == DeliveryStatus.ALREADY_PROCESSED.value:
                        stats["skipped"] += 1
                    elif status == DeliveryStatus.NEEDS_REVIEW.value:
                        stats["needs_review"] += 1
                    else:
                        stats["errors"] += 1
                except Exception as e:
                    stats["errors"] += 1
                    sys.stderr.write(f"[ERROR] Failed to download or sync file {meta.file_id} ({meta.name}): {e}\n")

        return stats
