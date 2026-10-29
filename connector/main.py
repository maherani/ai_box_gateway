# -*- coding: utf-8 -*-
"""CLI Entry point for AI Box Local Pull Connector."""

import argparse
import sys
from config import load_config, ConfigError
from source_webdav import WebDAVClient
from destination_adapter import LocalAiBoxAdapter
from state_store import StateStore
from core import CoreTransferEngine, ConcurrencyError

def main():
    parser = argparse.ArgumentParser(
        description="AI Box Local Pull Connector: Ingest plain text documents from Enterprise Archive."
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration settings and environment without issuing any network requests.",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Execute a single synchronization pass (for operational use).",
    )
    parser.add_argument(
        "--env-file",
        type=str,
        default=None,
        help="Optional path to custom environment file.",
    )

    args = parser.parse_args()

    if args.check_config:
        try:
            cfg = load_config(env_file_path=args.env_file)
            print("[CONFIG OK] Configuration successfully loaded and validated:")
            print(f"  - WebDAV Base URL:       {cfg.webdav_base_url}")
            print(f"  - WebDAV User:           {cfg.webdav_user}")
            print(f"  - WebDAV Folder:         {cfg.webdav_folder}")
            print(f"  - Destination Gateway:   {cfg.destination_gateway_url}")
            print(f"  - Destination Workspace: {cfg.destination_workspace_slug}")
            print(f"  - State File Path:       {cfg.state_file_path}")
            print(f"  - Max File Size:         {cfg.max_file_size} bytes")
            print(f"  - WebDAV Timeout:        {cfg.webdav_timeout}s")
            print(f"  - WebDAV Verify SSL:     {cfg.webdav_verify_ssl}")
            print("  - Password:              [REDACTED]")
            print(f"  - Destination API Key:   {'[CONFIGURED]' if cfg.destination_api_key else '[NOT SET]'}")
            sys.exit(0)
        except ConfigError as e:
            sys.stderr.write(f"[CONFIG ERROR] {e}\n")
            sys.exit(1)

    if args.run_once:
        try:
            cfg = load_config(env_file_path=args.env_file)
            state_store = StateStore(cfg.state_file_path)
            adapter = LocalAiBoxAdapter(cfg.destination_gateway_url, cfg.destination_workspace_slug, api_key=cfg.destination_api_key, timeout=cfg.webdav_timeout)
            webdav = WebDAVClient(
                cfg.webdav_base_url,
                cfg.webdav_user,
                cfg.webdav_password,
                timeout=cfg.webdav_timeout,
                verify_ssl=cfg.webdav_verify_ssl,
            )
            engine = CoreTransferEngine(cfg, state_store, adapter, source_client=webdav)
            print(f"[CONNECTOR STARTING] Syncing folder '{cfg.webdav_folder}'...")
            stats = engine.sync_folder()
            print(f"[CONNECTOR FINISHED] Sync summary: {stats}")
            sys.exit(0)
        except ConcurrencyError as e:
            sys.stderr.write(f"[CONCURRENCY BLOCKED] {e}\n")
            sys.exit(2)
        except Exception as e:
            sys.stderr.write(f"[EXECUTION FAILED] {e}\n")
            sys.exit(1)

    parser.print_help()
    sys.exit(0)

if __name__ == "__main__":
    main()
