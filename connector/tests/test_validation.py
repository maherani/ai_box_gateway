# -*- coding: utf-8 -*-
import unittest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from source_webdav import validate_text_file, validate_supported_text_file, compute_sha256, SUPPORTED_TEXT_EXTENSIONS

class TestSourceValidation(unittest.TestCase):
    def test_valid_txt_file(self):
        content = "پروژه هوش مصنوعی محلی - متن ساده تستی".encode("utf-8")
        res = validate_text_file("sample_persian.txt", content, max_size_bytes=2097152)
        self.assertTrue(res.is_valid)
        self.assertIsNone(res.error_message)

    def test_reject_non_txt_extension(self):
        res = validate_text_file("document.pdf", b"Some text", max_size_bytes=2097152)
        self.assertFalse(res.is_valid)
        self.assertIn("Only plain text files with .txt extension are permitted", res.error_message)

    def test_reject_oversized_file(self):
        content = b"A" * (2 * 1024 * 1024 + 1)
        res = validate_text_file("large.txt", content, max_size_bytes=2097152)
        self.assertFalse(res.is_valid)
        self.assertIn("exceeds maximum limit", res.error_message)

    def test_reject_disguised_pdf(self):
        content = b"%PDF-1.4 simulated pdf header"
        res = validate_text_file("fake_doc.txt", content, max_size_bytes=2097152)
        self.assertFalse(res.is_valid)
        self.assertIn("Disguised PDF", res.error_message)

    def test_reject_nul_bytes(self):
        content = b"hello\x00world"
        res = validate_text_file("binary.txt", content, max_size_bytes=2097152)
        self.assertFalse(res.is_valid)
        self.assertIn("NUL bytes", res.error_message)

    def test_reject_invalid_utf8(self):
        content = b"invalid utf8 \xff\xfe sequence"
        res = validate_text_file("bad_encoding.txt", content, max_size_bytes=2097152)
        self.assertFalse(res.is_valid)
        self.assertIn("UTF-8", res.error_message)

    def test_compute_sha256(self):
        content = b"test content for hash verification"
        h = compute_sha256(content)
        self.assertEqual(len(h), 64)
        self.assertEqual(h, "686f1f9a2bbc31017c303b69445d1ff3ecff95da427840c0ca491cb72ef14508")


    def test_supported_text_extensions_all_pass(self):
        content = "متن نمونه تستی معتبر".encode("utf-8")
        for ext in SUPPORTED_TEXT_EXTENSIONS:
            res = validate_supported_text_file(f"sample{ext}", content)
            self.assertTrue(res.is_valid, f"Failed for {ext}: {res.error_message}")
            self.assertIsNone(res.error_message)

    def test_supported_rejects_binary_extensions(self):
        content = b"Some plain text"
        for ext in [".pdf", ".docx", ".zip", ".exe", ".png"]:
            res = validate_supported_text_file(f"doc{ext}", content)
            self.assertFalse(res.is_valid)
            self.assertIn("Extension not permitted", res.error_message)

    def test_supported_rejects_disguised_zip_docx(self):
        content = b"PK\x03\x04 disguised docx"
        res = validate_supported_text_file("notes.md", content)
        self.assertFalse(res.is_valid)
        self.assertIn("Disguised ZIP/DOCX", res.error_message)

if __name__ == "__main__":
    unittest.main()
