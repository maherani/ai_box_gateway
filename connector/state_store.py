# -*- coding: utf-8 -*-
"""Atomic and persistent local JSON state storage for connector tracking."""

import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

class StateCorruptionError(Exception):
    """Raised when the state file contains invalid or corrupted data."""
    pass

@dataclass
class FileProcessingRecord:
    source_file_id: str
    filename: str
    content_hash: str
    status: str
    target_filename: Optional[str] = None
    doc_location: Optional[str] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    last_updated: float = 0.0

class StateStore:
    """Manages persistent tracking of synced files with atomic file updates."""

    def __init__(self, filepath: str = "sync_state.json"):
        # P1-5: Convert relative path to absolute based on connector directory
        if not os.path.isabs(filepath):
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.filepath = os.path.normpath(os.path.join(base_dir, filepath))
        else:
            self.filepath = os.path.normpath(filepath)

        self._records: Dict[str, FileProcessingRecord] = {}
        self.load()

    def load(self) -> None:
        if not os.path.isfile(self.filepath):
            self._records = {}
            return
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "processed_files" not in data:
                raise ValueError("State JSON structure is missing 'processed_files' key.")
            self._records = {
                k: FileProcessingRecord(**v) for k, v in data.get("processed_files", {}).items()
            }
        except Exception as e:
            # P3: Keep a recoverable backup, issue clear warning, and raise StateCorruptionError
            corrupt_path = f"{self.filepath}.corrupt.{int(time.time())}"
            try:
                os.rename(self.filepath, corrupt_path)
            except OSError:
                pass
            sys.stderr.write(
                f"[ERROR] State file '{self.filepath}' is corrupted: {e}. Backed up to '{corrupt_path}'.\n"
            )
            raise StateCorruptionError(
                f"State file '{self.filepath}' corrupted ({e}). Moved to '{corrupt_path}'. Stopping to prevent duplicate transfers."
            )

    def save(self) -> None:
        data = {
            "version": "1.0",
            "last_saved": time.time(),
            "processed_files": {k: asdict(v) for k, v in self._records.items()},
        }
        dirname = os.path.dirname(os.path.abspath(self.filepath)) or "."
        os.makedirs(dirname, exist_ok=True)

        # Atomic write: write to temp file with 0600 permissions, fsync, then replace
        fd, tmp_file = tempfile.mkstemp(dir=dirname, prefix="sync_state_", suffix=".tmp")
        try:
            try:
                os.fchmod(fd, 0o600)
            except OSError:
                pass
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_file, self.filepath)
        except Exception:
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass
            raise

    def get(self, file_id: str) -> Optional[FileProcessingRecord]:
        return self._records.get(str(file_id))

    def update_record(
        self,
        source_file_id: str,
        filename: str,
        content_hash: str,
        status: str,
        target_filename: Optional[str] = None,
        doc_location: Optional[str] = None,
        error_message: Optional[str] = None,
        increment_retry: bool = False,
    ) -> FileProcessingRecord:
        key = str(source_file_id)
        existing = self._records.get(key)
        if increment_retry:
            retry_count = (existing.retry_count + 1) if existing else 1
        else:
            retry_count = existing.retry_count if existing else 0

        record = FileProcessingRecord(
            source_file_id=key,
            filename=filename,
            content_hash=content_hash,
            status=status,
            target_filename=target_filename or (existing.target_filename if existing else None),
            doc_location=doc_location or (existing.doc_location if existing else None),
            error_message=error_message,
            retry_count=retry_count,
            last_updated=time.time(),
        )
        self._records[key] = record
        self.save()
        return record

    def all_records(self) -> Dict[str, FileProcessingRecord]:
        return dict(self._records)
