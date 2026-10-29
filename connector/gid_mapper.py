# -*- coding: utf-8 -*-
"""Authoritative GID to Workspace Slug mapping storage for AI Box.

Provides durable, atomic persistence of canonical Enterprise Archive Group ID (GID)
to AnythingLLM technical workspace slug mappings.
Ensures:
- Exact GID mapping without string guessing.
- Strict 1-to-1 mapping invariant (one slug cannot belong to multiple GIDs).
- Reverse lookup (slug -> GID) with fail-closed collision detection.
- Cross-process file locking (fcntl) and atomic file writes (os.replace).
"""

import json
import logging
import os
import tempfile
import threading
import time
from typing import Dict, Any, Optional, List

try:
    import fcntl
except ImportError:
    fcntl = None

logger = logging.getLogger("AIBox.GIDMapper")


class GIDWorkspaceMapper:
    """Manages persistent GID -> workspace_slug mappings in a shared JSON file."""

    def __init__(self, map_file_path: Optional[str] = None):
        if map_file_path is None:
            map_file_path = os.environ.get("GID_MAP_PATH", "./data/gid_workspace_map.json")
        self.map_file_path = os.path.abspath(map_file_path)
        self._lock = threading.Lock()
        self._ensure_dir()

    def _ensure_dir(self) -> None:
        target_dir = os.path.dirname(self.map_file_path)
        if target_dir and not os.path.exists(target_dir):
            try:
                os.makedirs(target_dir, exist_ok=True)
            except OSError as e:
                logger.warning("Could not create directory '%s': %s", target_dir, e)

    def _load_raw(self) -> Dict[str, Any]:
        """Loads raw mapping data from disk safely with optional shared lock."""
        if not os.path.exists(self.map_file_path):
            return {"mappings": {}}

        try:
            with open(self.map_file_path, "r", encoding="utf-8") as f:
                if fcntl:
                    try:
                        fcntl.flock(f.fileno(), fcntl.LOCK_SH)
                    except OSError:
                        pass
                try:
                    data = json.load(f)
                finally:
                    if fcntl:
                        try:
                            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                        except OSError:
                            pass

                if isinstance(data, dict) and isinstance(data.get("mappings"), dict):
                    return data
                logger.warning("Malformed structure in GID map file '%s', returning empty.", self.map_file_path)
                return {"mappings": {}}
        except (json.JSONDecodeError, OSError) as e:
            logger.error("Failed to read GID map file '%s': %s", self.map_file_path, e)
            return {"mappings": {}}

    def _save_raw(self, data: Dict[str, Any]) -> None:
        """Saves raw mapping data to disk atomically with fsync, lock, and rename."""
        self._ensure_dir()
        target_dir = os.path.dirname(self.map_file_path) or "."
        temp_file = tempfile.NamedTemporaryFile(
            mode="w",
            dir=target_dir,
            delete=False,
            encoding="utf-8",
            prefix=".gid_map_tmp_",
            suffix=".json",
        )
        temp_path = temp_file.name
        try:
            if fcntl:
                try:
                    fcntl.flock(temp_file.fileno(), fcntl.LOCK_EX)
                except OSError:
                    pass

            json.dump(data, temp_file, indent=2, ensure_ascii=False)
            temp_file.flush()
            os.fsync(temp_file.fileno())

            if fcntl:
                try:
                    fcntl.flock(temp_file.fileno(), fcntl.LOCK_UN)
                except OSError:
                    pass

            temp_file.close()
            os.replace(temp_path, self.map_file_path)
        except Exception as e:
            logger.error("Atomic write failed for '%s': %s", self.map_file_path, e)
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise

    def get_slug(self, gid: str) -> Optional[str]:
        """Returns the workspace slug mapped to the given GID, or None."""
        if not gid or not isinstance(gid, str):
            return None
        clean_gid = gid.strip()
        if not clean_gid:
            return None

        with self._lock:
            data = self._load_raw()
            entry = data["mappings"].get(clean_gid)
            if entry and isinstance(entry, dict):
                return entry.get("slug")
            return None

    def get_gid(self, slug: str) -> Optional[str]:
        """Reverse lookup: returns the canonical GID mapped to a workspace slug.

        Enforces strict fail-closed integrity:
        If multiple GIDs map to the same slug (corruption), returns None to prevent unauthorized access.
        """
        if not slug or not isinstance(slug, str):
            return None
        clean_slug = slug.strip()
        if not clean_slug:
            return None

        with self._lock:
            data = self._load_raw()
            matching_gids = [
                gid for gid, entry in data["mappings"].items()
                if isinstance(entry, dict) and entry.get("slug") == clean_slug
            ]

            if len(matching_gids) == 1:
                return matching_gids[0]
            if len(matching_gids) > 1:
                logger.error(
                    "Integrity violation: Workspace slug '%s' is mapped to multiple GIDs %s. Failing closed.",
                    clean_slug, matching_gids
                )
                return None
            return None

    def set_mapping(self, gid: str, slug: str, display_name: Optional[str] = None) -> None:
        """Atomically stores or updates a canonical GID -> workspace_slug mapping.

        Enforces 1-to-1 invariant: rejects re-assigning an already mapped slug to a different GID.
        """
        if not gid or not isinstance(gid, str) or not gid.strip():
            raise ValueError("GID must be a non-empty string.")
        if not slug or not isinstance(slug, str) or not slug.strip():
            raise ValueError("Workspace slug must be a non-empty string.")

        clean_gid = gid.strip()
        clean_slug = slug.strip()

        with self._lock:
            data = self._load_raw()

            # Enforce 1-to-1 mapping invariant
            for existing_gid, entry in data["mappings"].items():
                if existing_gid != clean_gid and isinstance(entry, dict):
                    if entry.get("slug") == clean_slug:
                        raise ValueError(
                            f"Slug collision: Workspace slug '{clean_slug}' is already mapped to GID '{existing_gid}'. "
                            f"Cannot reassign to GID '{clean_gid}' (1-to-1 mapping invariant violation)."
                        )

            now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            data["mappings"][clean_gid] = {
                "slug": clean_slug,
                "display_name": (display_name or clean_gid).strip(),
                "updated_at": now_iso,
            }
            self._save_raw(data)
            logger.info("Persisted GID mapping: GID '%s' -> slug '%s'", clean_gid, clean_slug)

    def remove_mapping(self, gid: str) -> bool:
        """Removes a GID mapping if it exists."""
        if not gid or not isinstance(gid, str):
            return False
        clean_gid = gid.strip()
        with self._lock:
            data = self._load_raw()
            if clean_gid in data["mappings"]:
                del data["mappings"][clean_gid]
                self._save_raw(data)
                return True
            return False

    def get_all_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Returns a snapshot of all active mappings."""
        with self._lock:
            return dict(self._load_raw().get("mappings", {}))