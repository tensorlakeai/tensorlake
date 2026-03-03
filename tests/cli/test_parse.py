import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

from tensorlake.cli import parse as parse_module


class TestParse(unittest.TestCase):
    def test_parse_emits_error_for_missing_local_file(self):
        with (
            patch.object(parse_module, "DocumentAI"),
            patch.object(parse_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                parse_module.parse(
                    "/tmp/does-not-exist.pdf", pages=None, ignore_cache=False
                )

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(emit.call_count, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("File not found", event["message"])

    def test_parse_uses_cache_for_url(self):
        cache = MagicMock()
        cache.get.return_value = "cached markdown"
        document_ai = MagicMock()

        with (
            patch.object(parse_module, "DocumentAI", return_value=document_ai),
            patch.object(parse_module, "KVCache", return_value=cache),
            patch.object(parse_module, "_emit") as emit,
        ):
            parse_module.parse(
                "https://example.com/file.pdf",
                pages="1-2",
                ignore_cache=False,
            )

        document_ai.upload.assert_not_called()
        document_ai.parse_and_wait.assert_not_called()

        cache_key = cache.get.call_args.args[0]
        self.assertIn("url:https://example.com/file.pdf", cache_key)
        self.assertIn("pages:1-2", cache_key)

        self.assertEqual(emit.call_args_list[0].args[0]["type"], "cached")
        self.assertEqual(emit.call_args_list[1].args[0]["type"], "output")
        self.assertEqual(emit.call_args_list[1].args[0]["content"], "cached markdown")

    def test_parse_uploads_and_parses_local_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "sample.txt"
            input_file.write_text("hello")

            cache = MagicMock()
            cache.get.return_value = None

            document_ai = MagicMock()
            document_ai.upload.return_value = "file-id"
            document_ai.parse_and_wait.return_value = SimpleNamespace(
                chunks=[
                    SimpleNamespace(content="chunk-1"),
                    SimpleNamespace(content="chunk-2"),
                ]
            )

            with (
                patch.object(parse_module, "DocumentAI", return_value=document_ai),
                patch.object(parse_module, "KVCache", return_value=cache),
                patch.object(parse_module, "_emit") as emit,
            ):
                parse_module.parse(str(input_file), pages=None, ignore_cache=False)

        document_ai.upload.assert_called_once_with(str(input_file))
        document_ai.parse_and_wait.assert_called_once_with(
            file="file-id",
            parsing_options=ANY,
            page_range=None,
        )

        cache.set.assert_called_once()
        cache_key, cache_value = cache.set.call_args.args
        self.assertIn("file:", cache_key)
        self.assertIn("pages:all", cache_key)
        self.assertEqual(cache_value, "chunk-1\n\nchunk-2")

        event_types = [call.args[0]["type"] for call in emit.call_args_list]
        self.assertEqual(event_types, ["status", "status", "output"])

    def test_parse_entrypoint_emits_unhandled_error(self):
        with (
            patch("sys.argv", ["tensorlake-parse", "file.pdf"]),
            patch.object(parse_module, "parse", side_effect=RuntimeError("boom")),
            patch.object(parse_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                parse_module.parse_entrypoint()

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(emit.call_count, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertEqual(event["message"], "RuntimeError: boom")


if __name__ == "__main__":
    unittest.main()
