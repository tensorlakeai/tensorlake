import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tensorlake.utils.cache import KVCache


class TestAtomicCacheWrites(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.cache = KVCache("parse", self.root)

    def assert_no_temporary_files(self):
        self.assertEqual(list(self.cache.ns_dir.glob(".cache-*")), [])

    def test_text_and_bytes_round_trip_independently(self):
        self.cache.set("entry", "first\nline café", encoding="utf-8")
        self.cache.set_bytes("entry", b"\x00\xffsecond")
        self.assertEqual(self.cache.get("entry"), "first\nline café")
        self.assertEqual(self.cache.get_bytes("entry"), b"\x00\xffsecond")
        self.assert_no_temporary_files()

    def test_custom_text_encoding_and_empty_value(self):
        self.cache.set("latin", "café", encoding="latin-1")
        self.assertEqual(self.cache.get("latin", encoding="latin-1"), "café")
        self.cache.set("latin", "", encoding="latin-1")
        self.assertEqual(self.cache.get("latin", encoding="latin-1"), "")
        self.cache.set_bytes("bytes", b"")
        self.assertEqual(self.cache.get_bytes("bytes"), b"")

    def test_failed_text_write_preserves_committed_value(self):
        self.cache.set("key", "committed")
        real_write = Path.write_text

        def fail_after_partial_write(path, value, **kwargs):
            real_write(path, "partial", **kwargs)
            raise OSError("injected disk write failure")

        with patch.object(Path, "write_text", fail_after_partial_write):
            self.assertIsNone(self.cache.set("key", "replacement"))
        self.assertEqual(self.cache.get("key"), "committed")
        self.assert_no_temporary_files()

    def test_failed_binary_write_preserves_committed_value(self):
        self.cache.set_bytes("key", b"committed")
        real_write = Path.write_bytes

        def fail_after_partial_write(path, value):
            real_write(path, b"partial")
            raise OSError("injected disk write failure")

        with patch.object(Path, "write_bytes", fail_after_partial_write):
            self.assertIsNone(self.cache.set_bytes("key", b"replacement"))
        self.assertEqual(self.cache.get_bytes("key"), b"committed")
        self.assert_no_temporary_files()

    def test_first_failed_write_remains_a_cache_miss(self):
        real_write = Path.write_bytes

        def fail_after_partial_write(path, value):
            real_write(path, b"partial")
            raise OSError("injected disk write failure")

        with patch.object(Path, "write_bytes", fail_after_partial_write):
            self.cache.set_bytes("new-key", b"complete")
        self.assertIsNone(self.cache.get_bytes("new-key"))
        self.assert_no_temporary_files()

    def test_failed_encoding_does_not_truncate_existing_entry(self):
        self.cache.set("key", "committed")
        self.cache.set("key", "café", encoding="ascii")
        self.assertEqual(self.cache.get("key"), "committed")
        self.assert_no_temporary_files()

    def test_replace_failure_preserves_old_entry_and_cleans_temporary(self):
        self.cache.set_bytes("key", b"committed")
        with patch("os.replace", side_effect=PermissionError("blocked")):
            self.cache.set_bytes("key", b"replacement")
        self.assertEqual(self.cache.get_bytes("key"), b"committed")
        self.assert_no_temporary_files()

    def test_reader_never_observes_partial_text(self):
        self.cache.set("key", "committed")
        real_write = Path.write_text
        observed = []

        def write_in_two_steps(path, value, **kwargs):
            real_write(path, "partial", **kwargs)
            observed.append(self.cache.get("key"))
            return real_write(path, value, **kwargs)

        with patch.object(Path, "write_text", write_in_two_steps):
            self.cache.set("key", "complete replacement")
        self.assertEqual(observed, ["committed"])
        self.assertEqual(self.cache.get("key"), "complete replacement")
        self.assert_no_temporary_files()

    def test_reader_never_observes_partial_bytes(self):
        self.cache.set_bytes("key", b"committed")
        real_write = Path.write_bytes
        observed = []

        def write_in_two_steps(path, value):
            real_write(path, b"partial")
            observed.append(self.cache.get_bytes("key"))
            return real_write(path, value)

        with patch.object(Path, "write_bytes", write_in_two_steps):
            self.cache.set_bytes("key", b"complete replacement")
        self.assertEqual(observed, [b"committed"])
        self.assertEqual(self.cache.get_bytes("key"), b"complete replacement")

    def test_new_value_is_invisible_until_publication(self):
        real_write = Path.write_text
        observed = []

        def inspect_during_write(path, value, **kwargs):
            result = real_write(path, value, **kwargs)
            observed.append(self.cache.get("key"))
            return result

        with patch.object(Path, "write_text", inspect_during_write):
            self.cache.set("key", "complete")
        self.assertEqual(observed, [None])
        self.assertEqual(self.cache.get("key"), "complete")

    def test_publication_uses_same_directory_and_complete_value(self):
        actual_replace = os.replace
        seen = []

        def inspect_replace(source, destination):
            source, destination = Path(source), Path(destination)
            self.assertEqual(source.parent, destination.parent)
            self.assertEqual(source.read_bytes(), b"complete")
            seen.append((source, destination))
            actual_replace(source, destination)

        with patch("os.replace", inspect_replace):
            self.cache.set_bytes("key", b"complete")
        self.assertEqual(len(seen), 1)
        self.assertEqual(self.cache.get_bytes("key"), b"complete")
        self.assert_no_temporary_files()

    def test_independent_keys_namespaces_delete_and_clear(self):
        other = KVCache("other", self.root)
        self.cache.set("one", "first")
        self.cache.set("two", "second")
        self.cache.set_bytes("one", b"first")
        other.set("one", "isolated")
        self.cache.delete("one")
        self.assertIsNone(self.cache.get("one"))
        self.assertIsNone(self.cache.get_bytes("one"))
        self.assertEqual(self.cache.get("two"), "second")
        self.cache.clear()
        self.assertIsNone(self.cache.get("two"))
        self.assertEqual(other.get("one"), "isolated")

    def test_unwritable_directory_is_still_best_effort(self):
        with patch.object(Path, "mkdir", side_effect=PermissionError("blocked")):
            self.assertIsNone(self.cache.set("key", "value"))
            self.assertIsNone(self.cache.set_bytes("key", b"value"))
        self.assertIsNone(self.cache.get("key"))
        self.assertIsNone(self.cache.get_bytes("key"))


if __name__ == "__main__":
    unittest.main()
