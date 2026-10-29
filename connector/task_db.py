# -*- coding: utf-8 -*-
"""Durable Task Database (Task DB) for AI Box.

Provides SQLite-based durable task tracking with WAL mode, synchronous=FULL
fsync guarantees, atomic deduplication, fencing-token leases, and state transitions.
"""

import datetime
import logging
import os
import sqlite3
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger("AIBox.TaskDB")

TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_PROCESSING = "PROCESSING"
TASK_STATUS_RETRY_WAIT = "RETRY_WAIT"
TASK_STATUS_INDEX_VERIFY_PENDING = "INDEX_VERIFY_PENDING"
TASK_STATUS_INDEXED = "INDEXED"
TASK_STATUS_SUPERSEDED = "SUPERSEDED"
TASK_STATUS_NEEDS_REVIEW = "NEEDS_REVIEW"

VALID_STATUSES = {
    TASK_STATUS_PENDING,
    TASK_STATUS_PROCESSING,
    TASK_STATUS_RETRY_WAIT,
    TASK_STATUS_INDEX_VERIFY_PENDING,
    TASK_STATUS_INDEXED,
    TASK_STATUS_SUPERSEDED,
    TASK_STATUS_NEEDS_REVIEW,
}


class TaskDB:
    """Manages persistent task state in SQLite with WAL and FULL durability."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = os.environ.get("TASK_DB_PATH", "./data/ai_box_tasks.db")
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self, "_conn") or self._conn is None:
            self._conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode = WAL;")
            self._conn.execute("PRAGMA synchronous = FULL;")
            self._conn.execute("PRAGMA busy_timeout = 10000;")
        return self._conn

    def init_db(self) -> None:
        """Create tables and indexes if they do not exist."""
        with self._get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_box_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT UNIQUE NOT NULL,
                file_id INTEGER NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                owner_uid TEXT NOT NULL,
                group_name TEXT DEFAULT 'General',
                mime_type TEXT DEFAULT 'text/plain',
                file_size INTEGER DEFAULT 0,
                sha256 TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 10,
                next_retry_at TIMESTAMP,
                lease_owner TEXT,
                lease_expires_at TIMESTAMP,
                last_error TEXT,
                created_at TIMESTAMP NOT NULL,
                updated_at TIMESTAMP NOT NULL,
                indexed_at TIMESTAMP,
                doc_location TEXT,
                cleanup_pending INTEGER DEFAULT 0,
                workspace_slug TEXT,
                cleanup_retry_count INTEGER NOT NULL DEFAULT 0
            );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON ai_box_tasks(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_file_id ON ai_box_tasks(file_id);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_lease ON ai_box_tasks(lease_owner, lease_expires_at);")
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN doc_location TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN cleanup_pending INTEGER DEFAULT 0;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN workspace_slug TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN cleanup_retry_count INTEGER NOT NULL DEFAULT 0;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN scope TEXT DEFAULT 'group';")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN workspace_name TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN old_workspace_slug TEXT;")
            except sqlite3.OperationalError:
                pass
            try:
                conn.execute("ALTER TABLE ai_box_tasks ADD COLUMN event_type TEXT DEFAULT 'file.created';")
            except sqlite3.OperationalError:
                pass
            conn.commit()

    def record_task(self, event_data: Dict[str, Any]) -> Tuple[bool, Optional[int]]:
        """Atomically record incoming Outbox event into Task DB.

        Returns (is_new, task_id).
        If event_id already exists, returns (False, existing_id).
        If a newer event for the same file_id is recorded, marks older PENDING,
        RETRY_WAIT, and in-flight PROCESSING tasks for that file as SUPERSEDED.
        """
        event_id = event_data["event_id"]
        file_id = int(event_data["file_id"])
        file_name = event_data.get("file_name", "")
        file_path = event_data.get("file_path", "")
        owner_uid = event_data.get("owner_uid", "")
        group_name = event_data.get("group_name", "General")
        mime_type = event_data.get("mime_type", "text/plain")
        file_size = int(event_data.get("file_size", 0))
        sha256 = event_data.get("sha256", "")
        scope = event_data.get("scope") or ("personal" if str(event_data.get("group_name", "")).lower() == "personal" else "group")
        workspace_slug = event_data.get("workspace_slug")
        workspace_name = event_data.get("workspace_name")
        old_workspace_slug = event_data.get("old_workspace_slug")
        event_type = event_data.get("event_type", "file.created")
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()

        with self._get_connection() as conn:
            # 1. Deduplication check
            cursor = conn.execute("SELECT id FROM ai_box_tasks WHERE event_id = ?", (event_id,))
            row = cursor.fetchone()
            if row:
                return False, row["id"]

            # 2. Supersede older uncompleted tasks (including in-flight PROCESSING) for the exact same file_id
            conn.execute("""
                UPDATE ai_box_tasks
                SET status = 'SUPERSEDED', lease_owner = NULL, updated_at = ?
                WHERE file_id = ? AND status IN ('PENDING', 'RETRY_WAIT', 'PROCESSING')
            """, (now, file_id))

            # 3. Insert new task atomically
            cursor = conn.execute("""
                INSERT INTO ai_box_tasks (
                    event_id, file_id, file_name, file_path, owner_uid,
                    group_name, scope, workspace_slug, workspace_name, old_workspace_slug,
                    event_type, mime_type, file_size, sha256, status,
                    retry_count, max_retries, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', 0, 10, ?, ?)
            """, (
                event_id, file_id, file_name, file_path, owner_uid,
                group_name, scope, workspace_slug, workspace_name, old_workspace_slug,
                event_type, mime_type, file_size, sha256, now, now
            ))
            conn.commit()
            return True, cursor.lastrowid

    def is_task_current(self, task_id: int, worker_id: str) -> bool:
        """Check if task is actively owned by worker and not superseded by a newer version.

        Guarantees that a slower worker cannot deliver an obsolete version of a file
        to the destination adapter if a newer version has already arrived.
        """
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT file_id, status FROM ai_box_tasks
                WHERE id = ? AND lease_owner = ?
            """, (task_id, worker_id))
            row = cursor.fetchone()
            if not row or row["status"] != TASK_STATUS_PROCESSING:
                return False

            file_id = row["file_id"]
            # Check if any newer task exists for the same file_id
            newer = conn.execute("""
                SELECT id FROM ai_box_tasks
                WHERE file_id = ? AND id > ?
                LIMIT 1
            """, (file_id, task_id)).fetchone()

            return newer is None

    def set_task_doc_location(
        self,
        task_id: int,
        doc_location: str,
        workspace_slug: Optional[str] = None,
    ) -> None:
        """Records the unique destination document location and workspace slug for a task."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE ai_box_tasks
                SET doc_location = ?,
                    workspace_slug = coalesce(?, workspace_slug),
                    updated_at = ?
                WHERE id = ?
            """, (doc_location, workspace_slug, now, task_id))
            conn.commit()

    def mark_task_cleanup_pending(
        self,
        task_id: int,
        error: Optional[str] = None,
        retry_delay_seconds: int = 60,
        max_cleanup_retries: int = 5,
    ) -> None:
        """Marks a superseded task as requiring document cleanup retry.
        Uses an independent cleanup_retry_count, preserving the main processing retry_count.
        When max_cleanup_retries is reached, transitions to NEEDS_REVIEW and clears next_retry_at
        so it is excluded from auto-retry while preserving cleanup_pending and last_error.
        """
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now = now_dt.isoformat()
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT cleanup_retry_count, status FROM ai_box_tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            current_retries = (row["cleanup_retry_count"] if row else 0) or 0
            new_cleanup_retries = current_retries + 1

            if new_cleanup_retries >= max_cleanup_retries:
                status = TASK_STATUS_NEEDS_REVIEW
                # Stop automatic retries by leaving next_retry_at NULL
                next_retry = None
            else:
                status = row["status"] if row and row["status"] != TASK_STATUS_PROCESSING else TASK_STATUS_SUPERSEDED
                next_retry = (now_dt + datetime.timedelta(seconds=retry_delay_seconds)).isoformat()

            conn.execute("""
                UPDATE ai_box_tasks
                SET cleanup_pending = 1,
                    cleanup_retry_count = ?,
                    status = ?,
                    next_retry_at = ?,
                    last_error = ?,
                    updated_at = ?
                WHERE id = ?
            """, (new_cleanup_retries, status, next_retry, error, now, task_id))
            conn.commit()

    def mark_task_cleanup_completed(self, task_id: int) -> None:
        """Marks a superseded task's document cleanup as successfully completed."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE ai_box_tasks
                SET cleanup_pending = 0,
                    next_retry_at = NULL,
                    last_error = NULL,
                    updated_at = ?
                WHERE id = ?
            """, (now, task_id))
            conn.commit()

    def get_pending_cleanup_tasks(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns superseded tasks with pending document cleanup ready for retry.
        Excludes NEEDS_REVIEW records whose cleanup retries have been exhausted.
        """
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM ai_box_tasks
                WHERE cleanup_pending = 1
                  AND status != 'NEEDS_REVIEW'
                  AND doc_location IS NOT NULL
                  AND next_retry_at IS NOT NULL
                  AND next_retry_at <= ?
                ORDER BY id ASC
                LIMIT ?
            """, (now, limit))
            return [dict(r) for r in cursor.fetchall()]

    def get_current_task_for_file(self, file_id: int) -> Optional[Dict[str, Any]]:
        """Returns the latest active/valid task for a file (excluding superseded tasks)."""
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT * FROM ai_box_tasks
                WHERE file_id = ? AND status != 'SUPERSEDED'
                ORDER BY id DESC
                LIMIT 1
            """, (file_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def claim_task(self, worker_id: str, lease_duration_seconds: int = 300) -> Optional[Dict[str, Any]]:
        """Atomically claim the next runnable task with a fencing lease token."""
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now = now_dt.isoformat()
        lease_expires = (now_dt + datetime.timedelta(seconds=lease_duration_seconds)).isoformat()

        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT id FROM ai_box_tasks
                WHERE status = 'PENDING'
                   OR (status = 'RETRY_WAIT' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                   OR (status = 'PROCESSING' AND lease_expires_at < ?)
                ORDER BY id ASC
                LIMIT 1
            """, (now, now))
            row = cursor.fetchone()
            if not row:
                return None

            task_id = row["id"]
            update_cursor = conn.execute("""
                UPDATE ai_box_tasks
                SET status = 'PROCESSING',
                    lease_owner = ?,
                    lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ? AND (
                    status = 'PENDING'
                    OR (status = 'RETRY_WAIT' AND (next_retry_at IS NULL OR next_retry_at <= ?))
                    OR (status = 'PROCESSING' AND lease_expires_at < ?)
                )
            """, (worker_id, lease_expires, now, task_id, now, now))
            conn.commit()

            if update_cursor.rowcount == 0:
                return None

            return self.get_task(task_id)

    def update_task_status(
        self,
        task_id: int,
        worker_id: str,
        new_status: str,
        error: Optional[str] = None,
        retry_delay_seconds: int = 60,
        doc_location: Optional[str] = None,
        workspace_slug: Optional[str] = None,
    ) -> bool:
        """Update task status verifying lease ownership (fencing token) and active state."""
        if new_status not in VALID_STATUSES:
            raise ValueError(f"Invalid task status: {new_status}")

        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now = now_dt.isoformat()

        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT retry_count, max_retries, status FROM ai_box_tasks WHERE id = ? AND lease_owner = ?",
                (task_id, worker_id),
            )
            row = cursor.fetchone()
            if not row or row["status"] != TASK_STATUS_PROCESSING:
                logger.warning(
                    "Fencing/State rejection: Worker %s cannot update task %d (lease expired, superseded, or not owner)",
                    worker_id, task_id,
                )
                return False

            retry_count = row["retry_count"]
            max_retries = row["max_retries"]
            next_retry_at = None
            indexed_at = None

            if new_status == TASK_STATUS_RETRY_WAIT:
                retry_count += 1
                if retry_count >= max_retries:
                    new_status = TASK_STATUS_NEEDS_REVIEW
                    logger.error("Task %d exceeded max retries (%d). Transitioning to %s", task_id, max_retries, new_status)
                else:
                    next_retry_at = (now_dt + datetime.timedelta(seconds=retry_delay_seconds)).isoformat()
            elif new_status == TASK_STATUS_INDEXED:
                indexed_at = now

            conn.execute("""
                UPDATE ai_box_tasks
                SET status = ?,
                    retry_count = ?,
                    next_retry_at = ?,
                    indexed_at = coalesce(?, indexed_at),
                    doc_location = coalesce(?, doc_location),
                    workspace_slug = coalesce(?, workspace_slug),
                    last_error = ?,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ? AND lease_owner = ? AND status = 'PROCESSING'
            """, (new_status, retry_count, next_retry_at, indexed_at, doc_location, workspace_slug, error, now, task_id, worker_id))
            conn.commit()
            return True

    def mark_task_indexed(self, task_id: int) -> bool:
        """Transitions a task from INDEX_VERIFY_PENDING to INDEXED after physical vector presence is independently confirmed."""
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with self._get_connection() as conn:
            cur = conn.execute(
                """
                UPDATE ai_box_tasks
                SET status = 'INDEXED',
                    indexed_at = ?,
                    updated_at = ?
                WHERE id = ? AND status = 'INDEX_VERIFY_PENDING'
                """,
                (now, now, task_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def get_tasks_by_status(self, status: str):
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM ai_box_tasks WHERE status = ?", (status,))
            return [dict(row) for row in cursor.fetchall()]

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM ai_box_tasks WHERE id = ?", (task_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_task_by_event_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cursor = conn.execute("SELECT * FROM ai_box_tasks WHERE event_id = ?", (event_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
