# -*- coding: utf-8 -*-
"""Configuration loader and validator for AI Box Local Pull Connector."""

import os
import urllib.parse
from dataclasses import dataclass, field
from typing import Optional, Dict

class ConfigError(ValueError):
    """Raised when connector configuration is missing or invalid."""
    pass

@dataclass(frozen=True)
class ConnectorConfig:
    webdav_base_url: str
    webdav_user: str
    webdav_password: str = field(repr=False)
    webdav_folder: str
    destination_gateway_url: str
    destination_workspace_slug: str
    max_file_size: int = 2 * 1024 * 1024  # 2 MiB default
    poll_interval_seconds: int = 30
    state_file_path: str = "sync_state.json"
    webdav_timeout: int = 60
    webdav_verify_ssl: bool = True
    destination_api_key: Optional[str] = field(default=None, repr=False)

def parse_env_file(filepath: str) -> Dict[str, str]:
    env_vars = {}
    if not os.path.isfile(filepath):
        return env_vars
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                env_vars[k.strip()] = v.strip().strip("\"'")
    return env_vars

def load_config(env_dict: Optional[Dict[str, str]] = None, env_file_path: Optional[str] = None) -> ConnectorConfig:
    env = {}
    if env_file_path and os.path.isfile(env_file_path):
        env.update(parse_env_file(env_file_path))
    env.update(os.environ)
    if env_dict:
        env.update(env_dict)

    webdav_base_url = env.get("WEBDAV_BASE_URL", "").strip()
    webdav_user = env.get("WEBDAV_USER", "").strip()
    webdav_password = env.get("WEBDAV_PASSWORD", "").strip()
    webdav_folder = env.get("WEBDAV_FOLDER", "").strip()

    if not webdav_base_url:
        raise ConfigError("Missing required configuration: WEBDAV_BASE_URL")
    if not webdav_user:
        raise ConfigError("Missing required configuration: WEBDAV_USER. Must specify a dedicated archive user.")
    if not webdav_password:
        raise ConfigError("Missing required configuration: WEBDAV_PASSWORD. Must specify user app password or secret.")
    if not webdav_folder:
        raise ConfigError("Missing required configuration: WEBDAV_FOLDER. Explicit target folder is required to prevent unbounded scanning.")

    # Validate scheme and remote HTTP
    parsed_base = urllib.parse.urlparse(webdav_base_url)
    if not parsed_base.scheme or parsed_base.scheme not in ("http", "https"):
        raise ConfigError(f"Invalid WEBDAV_BASE_URL scheme '{parsed_base.scheme}'. Must be 'http' or 'https'.")
    if not parsed_base.netloc:
        raise ConfigError(f"Invalid WEBDAV_BASE_URL '{webdav_base_url}'. Missing host/network location.")

    hostname = (parsed_base.hostname or "").lower()
    is_loopback = hostname in ("localhost", "127.0.0.1", "::1", "testserver")
    if parsed_base.scheme == "http" and not is_loopback:
        raise ConfigError(
            f"Insecure HTTP scheme is only permitted for local loopback testing. Use HTTPS for remote host '{hostname}'."
        )

    gateway_url = env.get("DESTINATION_GATEWAY_URL", "http://127.0.0.1:3001").strip().rstrip("/")
    workspace_slug = env.get("DESTINATION_WORKSPACE_SLUG", "ai-box-test").strip()
    if not workspace_slug:
        raise ConfigError("Missing required configuration: DESTINATION_WORKSPACE_SLUG")

    try:
        max_size = int(env.get("MAX_FILE_SIZE_BYTES", str(2 * 1024 * 1024)))
        if max_size <= 0:
            raise ValueError
    except ValueError:
        raise ConfigError("MAX_FILE_SIZE_BYTES must be a positive integer")

    try:
        poll_interval = int(env.get("POLL_INTERVAL_SECONDS", "30"))
        if poll_interval < 1:
            raise ValueError
    except ValueError:
        raise ConfigError("POLL_INTERVAL_SECONDS must be a positive integer")

    try:
        webdav_timeout = int(env.get("WEBDAV_TIMEOUT", "60"))
        if webdav_timeout <= 0:
            raise ValueError
    except ValueError:
        raise ConfigError("WEBDAV_TIMEOUT must be a positive integer")

    verify_ssl_raw = env.get("WEBDAV_VERIFY_SSL", "true").strip().lower()
    webdav_verify_ssl = verify_ssl_raw not in ("false", "0", "no")

    raw_state_path = env.get("STATE_FILE_PATH", "sync_state.json").strip()
    if not raw_state_path:
        raw_state_path = "sync_state.json"

    # P1-5: Convert relative state path to absolute path based on connector directory
    if not os.path.isabs(raw_state_path):
        module_dir = os.path.dirname(os.path.abspath(__file__))
        state_file_path = os.path.normpath(os.path.join(module_dir, raw_state_path))
    else:
        state_file_path = os.path.normpath(raw_state_path)

    dest_api_key = env.get("DESTINATION_API_KEY", "").strip() or env.get("ANYTHINGLLM_API_KEY", "").strip() or None

    return ConnectorConfig(
        webdav_base_url=webdav_base_url.rstrip("/"),
        webdav_user=webdav_user,
        webdav_password=webdav_password,
        webdav_folder=webdav_folder.strip("/"),
        destination_gateway_url=gateway_url,
        destination_workspace_slug=workspace_slug,
        max_file_size=max_size,
        poll_interval_seconds=poll_interval,
        state_file_path=state_file_path,
        webdav_timeout=webdav_timeout,
        webdav_verify_ssl=webdav_verify_ssl,
        destination_api_key=dest_api_key,
    )
