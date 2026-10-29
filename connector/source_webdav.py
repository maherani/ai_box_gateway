# -*- coding: utf-8 -*-
"""WebDAV Client and Content Validation for Enterprise Archive."""

import base64
import hashlib
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Optional, Tuple

class WebDAVError(Exception):
    """Base exception for WebDAV operations."""
    pass

class AuthenticationError(WebDAVError):
    """Raised on HTTP 401 Unauthorized."""
    pass

class ResourceNotFoundError(WebDAVError):
    """Raised on HTTP 404 Not Found."""
    pass

class SecurityError(WebDAVError):
    """Raised when security boundaries (origin, traversal, credentials) are violated."""
    pass

@dataclass(frozen=True)
class WebDAVFileMetadata:
    file_id: str
    href: str
    name: str
    size: int
    last_modified: str
    content_type: str = ""

@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    error_message: Optional[str] = None

SUPPORTED_TEXT_EXTENSIONS = (
    ".txt", ".md", ".log", ".csv", ".json", ".xml", ".yaml", ".yml"
)

def validate_supported_text_file(
    filename: str,
    content: bytes,
    max_size_bytes: int = 2 * 1024 * 1024,
    allowed_extensions: Tuple[str, ...] = SUPPORTED_TEXT_EXTENSIONS
) -> ValidationResult:
    """Strictly validates that content satisfies AI Box text policies for supported text formats."""
    lower_fn = filename.lower()
    if not any(lower_fn.endswith(ext) for ext in allowed_extensions):
        allowed_str = ", ".join(allowed_extensions)
        return ValidationResult(
            False,
            f"File '{filename}' rejected: Extension not permitted. Allowed text extensions: {allowed_str}."
        )

    if len(content) > max_size_bytes:
        return ValidationResult(
            False,
            f"File '{filename}' rejected: File size ({len(content)} bytes) exceeds maximum limit of {max_size_bytes} bytes."
        )

    if content.startswith(b"%PDF") or b"%PDF-" in content[:1024]:
        return ValidationResult(
            False,
            f"File '{filename}' rejected: Disguised PDF content detected. PDFs are not allowed."
        )

    if content.startswith(b"PK\x03\x04"):
        return ValidationResult(
            False,
            f"File '{filename}' rejected: Disguised ZIP/DOCX content detected. Binary packages are not allowed."
        )

    if b"\x00" in content:
        return ValidationResult(
            False,
            f"File '{filename}' rejected: Binary content (NUL bytes) detected. Only plain text is allowed."
        )

    try:
        content.decode("utf-8")
    except UnicodeDecodeError as e:
        return ValidationResult(
            False,
            f"File '{filename}' rejected: Content is not valid UTF-8 text ({e})."
        )

    return ValidationResult(True, None)

def validate_text_file(filename: str, content: bytes, max_size_bytes: int = 2 * 1024 * 1024) -> ValidationResult:
    """Strictly validates legacy .txt files for folder sync to avoid ingesting non-txt files during folder scan."""
    if not filename.lower().endswith(".txt"):
        return ValidationResult(False, f"File '{filename}' rejected: Only plain text files with .txt extension are permitted.")
    return validate_supported_text_file(filename, content, max_size_bytes, allowed_extensions=(".txt",))

def compute_sha256(content: bytes) -> str:
    """Calculates full 64-char hex SHA-256 digest of file payload."""
    return hashlib.sha256(content).hexdigest()

def parse_webdav_propfind_xml(xml_bytes: bytes, target_folder_path: Optional[str] = None) -> List[WebDAVFileMetadata]:
    """Parses Nextcloud WebDAV PROPFIND multistatus XML response.
    
    Extracts only successful (HTTP 200) propstat properties and strictly requires oc:fileid.
    """
    namespaces = {
        "d": "DAV:",
        "oc": "http://owncloud.org/ns",
    }
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise ValueError(f"Failed to parse WebDAV XML response: {e}")

    files = []
    for resp in root.findall("d:response", namespaces):
        href_el = resp.find("d:href", namespaces)
        href = href_el.text.strip() if href_el is not None and href_el.text else ""
        if not href:
            continue

        # Check for path traversal or dangerous segments in href
        decoded_href = urllib.parse.unquote(href)
        if "/../" in decoded_href or decoded_href.endswith("/.."):
            continue

        # Find successful propstat (HTTP 200)
        successful_props = None
        for propstat in resp.findall("d:propstat", namespaces):
            status_el = propstat.find("d:status", namespaces)
            if status_el is not None and status_el.text and "200" in status_el.text:
                prop_el = propstat.find("d:prop", namespaces)
                if prop_el is not None:
                    successful_props = prop_el
                    break

        if successful_props is None:
            continue

        # Skip directories / collections
        resourcetype = successful_props.find("d:resourcetype", namespaces)
        if resourcetype is not None and resourcetype.find("d:collection", namespaces) is not None:
            continue

        # Strictly require Nextcloud oc:fileid
        fileid_el = successful_props.find("oc:fileid", namespaces)
        file_id = fileid_el.text.strip() if fileid_el is not None and fileid_el.text else ""
        if not file_id:
            # Mandatory oc:fileid is missing; do not falsely treat as valid
            continue

        length_el = successful_props.find("d:getcontentlength", namespaces)
        try:
            size = int(length_el.text.strip()) if length_el is not None and length_el.text else 0
        except ValueError:
            size = 0

        mtime_el = successful_props.find("d:getlastmodified", namespaces)
        last_modified = mtime_el.text.strip() if mtime_el is not None and mtime_el.text else ""

        ctype_el = successful_props.find("d:getcontenttype", namespaces)
        content_type = ctype_el.text.strip() if ctype_el is not None and ctype_el.text else ""

        # Extract filename from href
        name = decoded_href.rstrip("/").split("/")[-1] if decoded_href else ""

        if name:
            files.append(WebDAVFileMetadata(
                file_id=file_id,
                href=href,
                name=name,
                size=size,
                last_modified=last_modified,
                content_type=content_type,
            ))

    return files

class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Prevents leaking Authorization header across redirects to different origins."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        new_req = super().redirect_request(req, fp, code, msg, headers, newurl)
        if new_req is None:
            return None

        orig_parsed = urllib.parse.urlparse(req.full_url)
        new_parsed = urllib.parse.urlparse(newurl)

        # Drop Authorization if origin changes
        if (orig_parsed.scheme, orig_parsed.netloc) != (new_parsed.scheme, new_parsed.netloc):
            new_req.headers.pop("Authorization", None)
            new_req.headers.pop("authorization", None)
            raise SecurityError(f"Redirect to external origin '{new_parsed.netloc}' is rejected to prevent credential leak.")
        return new_req

class WebDAVClient:
    """Official WebDAV Client for Enterprise Archive file discovery and download."""

    PROPFIND_XML_BODY = (
        '<?xml version="1.0" encoding="utf-8" ?>\n'
        '<d:propfind xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">\n'
        '  <d:prop>\n'
        '    <d:resourcetype/>\n'
        '    <d:getcontentlength/>\n'
        '    <d:getlastmodified/>\n'
        '    <d:getcontenttype/>\n'
        '    <oc:fileid/>\n'
        '  </d:prop>\n'
        '</d:propfind>\n'
    ).encode("utf-8")

    def __init__(
        self,
        base_url: str,
        user: str,
        password: str,
        timeout: int = 60,
        verify_ssl: bool = True,
    ):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout = timeout
        self.verify_ssl = verify_ssl

        auth_str = f"{self.user}:{self.password}"
        self._auth_header = f"Basic {base64.b64encode(auth_str.encode('utf-8')).decode('ascii')}"

        # Setup SSL context for HTTPS
        if self.verify_ssl:
            self._ssl_context = ssl.create_default_context()
        else:
            self._ssl_context = ssl._create_unverified_context()

        # Build custom opener with SafeRedirectHandler
        self._opener = urllib.request.build_opener(
            SafeRedirectHandler,
            urllib.request.HTTPSHandler(context=self._ssl_context),
        )

    def _build_folder_url(self, folder_path: str) -> str:
        clean_folder = folder_path.strip("/")
        # URL-encode each folder segment safely for spaces and Persian characters
        encoded_segments = [urllib.parse.quote(seg) for seg in clean_folder.split("/") if seg]
        encoded_folder = "/".join(encoded_segments)
        return f"{self.base_url}/{encoded_folder}/"

    def list_folder(self, folder_path: str) -> List[WebDAVFileMetadata]:
        """Issues an explicit PROPFIND request requesting oc:fileid and metadata."""
        folder_url = self._build_folder_url(folder_path)
        headers = {
            "Authorization": self._auth_header,
            "Depth": "1",
            "Content-Type": "application/xml; charset=utf-8",
            "Accept": "application/xml, text/xml",
        }
        req = urllib.request.Request(folder_url, data=self.PROPFIND_XML_BODY, headers=headers, method="PROPFIND")

        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                xml_data = resp.read()
                return parse_webdav_propfind_xml(xml_data, folder_path)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise AuthenticationError(f"WebDAV authentication failed (HTTP 401) for user '{self.user}'.")
            if e.code == 404:
                raise ResourceNotFoundError(f"WebDAV target folder '{folder_path}' not found (HTTP 404).")
            raise WebDAVError(f"WebDAV PROPFIND failed (HTTP {e.code}): {e.reason}")
        except urllib.error.URLError as e:
            raise WebDAVError(f"Network error connecting to WebDAV server: {e.reason}")

    def download_file(self, file_href: str, max_bytes: int = 2 * 1024 * 1024) -> bytes:
        """Downloads file content using GET and strictly enforces actual byte count."""
        resolved_url = urllib.parse.urljoin(self.base_url + "/", file_href)

        # Ensure resolved URL stays on same origin
        orig_p = urllib.parse.urlparse(self.base_url)
        res_p = urllib.parse.urlparse(resolved_url)
        if (orig_p.scheme, orig_p.netloc) != (res_p.scheme, res_p.netloc):
            raise SecurityError(f"Cross-origin download is prohibited: target '{res_p.netloc}' != base '{orig_p.netloc}'.")

        headers = {
            "Authorization": self._auth_header,
            "Accept": "*/*",
        }
        req = urllib.request.Request(resolved_url, headers=headers, method="GET")

        try:
            with self._opener.open(req, timeout=self.timeout) as resp:
                content_len_hdr = resp.headers.get("Content-Length")
                if content_len_hdr:
                    try:
                        if int(content_len_hdr) > max_bytes:
                            raise ValueError(
                                f"Declared Content-Length ({content_len_hdr} bytes) exceeds maximum permitted size of {max_bytes} bytes."
                            )
                    except ValueError as e:
                        if "exceeds maximum" in str(e):
                            raise

                chunks = []
                total_bytes = 0
                chunk_size = 65536
                while True:
                    chunk = resp.read(chunk_size)
                    if not chunk:
                        break
                    total_bytes += len(chunk)
                    if total_bytes > max_bytes:
                        raise ValueError(f"Download exceeded maximum permitted size of {max_bytes} bytes (read {total_bytes} bytes).")
                    chunks.append(chunk)
                return b"".join(chunks)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise AuthenticationError(f"WebDAV download unauthorized (HTTP 401).")
            if e.code == 404:
                raise ResourceNotFoundError(f"WebDAV file '{file_href}' not found (HTTP 404).")
            raise WebDAVError(f"WebDAV download failed (HTTP {e.code}): {e.reason}")
        except urllib.error.URLError as e:
            raise WebDAVError(f"Network error during WebDAV download: {e.reason}")
