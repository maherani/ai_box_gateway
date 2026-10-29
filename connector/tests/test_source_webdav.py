# -*- coding: utf-8 -*-
import http.server
import os
import sys
import threading
import unittest
import urllib.parse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_webdav import (
    WebDAVClient,
    parse_webdav_propfind_xml,
    AuthenticationError,
    ResourceNotFoundError,
    WebDAVError,
    SecurityError,
)

class MockWebDAVHandler(http.server.BaseHTTPRequestHandler):
    """Local mock WebDAV HTTP server for offline testing."""

    def log_message(self, format, *args):
        pass  # Quiet logging during test runs

    def do_PROPFIND(self):
        auth = self.headers.get("Authorization", "")
        if auth != "Basic dGVzdF91c2VyOnNlY3JldA==":
            self.send_response(401)
            self.end_headers()
            return

        depth = self.headers.get("Depth", "infinity")
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        # Verify client sent explicit XML requesting oc:fileid
        if "<oc:fileid/>" not in body or "<d:propfind" not in body:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Missing oc:fileid in PROPFIND request")
            return

        unquoted_path = urllib.parse.unquote(self.path)
        if "not_found" in unquoted_path:
            self.send_response(404)
            self.end_headers()
            return

        # Return multistatus XML with 200 OK propstat and 404 propstat
        xml_resp = """<?xml version="1.0" encoding="utf-8"?>
<d:multistatus xmlns:d="DAV:" xmlns:oc="http://owncloud.org/ns">
  <d:response>
    <d:href>/remote.php/dav/files/user/inbox/</d:href>
    <d:propstat>
      <d:status>HTTP/1.1 200 OK</d:status>
      <d:prop><d:resourcetype><d:collection/></d:resourcetype></d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/remote.php/dav/files/user/inbox/%DA%AF%D8%B2%D8%A7%D8%B1%D8%B4%20%D9%BE%D8%B1%D9%88%DA%98%D9%87.txt</d:href>
    <d:propstat>
      <d:status>HTTP/1.1 200 OK</d:status>
      <d:prop>
        <d:resourcetype/>
        <oc:fileid>660</oc:fileid>
        <d:getcontentlength>1024</d:getcontentlength>
        <d:getlastmodified>Sat, 17 Oct 2026 18:00:00 GMT</d:getlastmodified>
        <d:getcontenttype>text/plain</d:getcontenttype>
      </d:prop>
    </d:propstat>
    <d:propstat>
      <d:status>HTTP/1.1 404 Not Found</d:status>
      <d:prop><oc:tags/></d:prop>
    </d:propstat>
  </d:response>
  <d:response>
    <d:href>/remote.php/dav/files/user/inbox/missing_id.txt</d:href>
    <d:propstat>
      <d:status>HTTP/1.1 200 OK</d:status>
      <d:prop>
        <d:resourcetype/>
        <d:getcontentlength>512</d:getcontentlength>
      </d:prop>
    </d:propstat>
  </d:response>
</d:multistatus>""".encode("utf-8")

        self.send_response(207, "Multi-Status")
        self.send_header("Content-Type", "application/xml; charset=utf-8")
        self.send_header("Content-Length", str(len(xml_resp)))
        self.end_headers()
        self.wfile.write(xml_resp)

    def do_GET(self):
        auth = self.headers.get("Authorization", "")
        if auth != "Basic dGVzdF91c2VyOnNlY3JldA==":
            self.send_response(401)
            self.end_headers()
            return

        unquoted_path = urllib.parse.unquote(self.path)
        if "not_found" in unquoted_path:
            self.send_response(404)
            self.end_headers()
            return

        if "oversized_header" in unquoted_path:
            self.send_response(200)
            self.send_header("Content-Length", str(3 * 1024 * 1024))
            self.end_headers()
            return

        if "oversized_stream" in unquoted_path:
            self.send_response(200)
            self.end_headers()
            try:
                # Stream chunks until client aborts
                for _ in range(50):
                    self.wfile.write(b"X" * 65536)
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            return

        if "redirect_external" in unquoted_path:
            self.send_response(302)
            self.send_header("Location", "http://evil.external.com/stolen")
            self.end_headers()
            return

        data = "سلام جهان - محتوای تستی معتبر".encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

class TestWebDAVClientOffline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), MockWebDAVHandler)
        cls.port = cls.server.server_address[1]
        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        self.base_url = f"http://127.0.0.1:{self.port}/remote.php/dav/files/user"
        self.client = WebDAVClient(self.base_url, user="test_user", password="secret")

    def test_list_folder_success_and_persian_filename(self):
        files = self.client.list_folder("inbox")
        # missing_id.txt was skipped because oc:fileid was missing
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].file_id, "660")
        self.assertEqual(files[0].name, "گزارش پروژه.txt")
        self.assertEqual(files[0].size, 1024)

    def test_list_folder_unauthorized(self):
        bad_client = WebDAVClient(self.base_url, user="wrong_user", password="wrong_password")
        with self.assertRaises(AuthenticationError):
            bad_client.list_folder("inbox")

    def test_list_folder_not_found(self):
        with self.assertRaises(ResourceNotFoundError):
            self.client.list_folder("not_found_folder")

    def test_download_file_success(self):
        content = self.client.download_file("/remote.php/dav/files/user/inbox/test.txt")
        self.assertIn("سلام جهان", content.decode("utf-8"))

    def test_download_file_oversized_header_aborts(self):
        with self.assertRaises(ValueError) as ctx:
            self.client.download_file("/remote.php/dav/files/user/inbox/oversized_header.txt", max_bytes=2*1024*1024)
        self.assertIn("exceeds maximum permitted size", str(ctx.exception))

    def test_download_file_oversized_stream_aborts(self):
        with self.assertRaises(ValueError) as ctx:
            self.client.download_file("/remote.php/dav/files/user/inbox/oversized_stream.txt", max_bytes=2*1024*1024)
        self.assertIn("exceeded maximum permitted size", str(ctx.exception))

    def test_redirect_to_external_origin_blocked(self):
        with self.assertRaises(SecurityError):
            self.client.download_file("/remote.php/dav/files/user/inbox/redirect_external")

if __name__ == "__main__":
    unittest.main()
