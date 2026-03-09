import os
import unittest
from unittest.mock import MagicMock, patch

from tensorlake.cli import build_images as build_images_module


def _make_image(name="my-image", tag="latest", base_image="python:3.11"):
    image = MagicMock()
    image.name = name
    image.tag = tag
    image._base_image = base_image
    image._build_operations = []
    return image


def _make_image_info(name="my-image", tag="latest", base_image="python:3.11"):
    info = MagicMock()
    info.image = _make_image(name=name, tag=tag, base_image=base_image)
    return info


class TestBuildImages(unittest.TestCase):
    def test_emits_user_friendly_import_error(self):
        with (
            patch.object(
                build_images_module, "load_code", side_effect=ImportError("secret")
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name=None,
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("failed to import application file", event["message"])
        self.assertNotIn("secret", event["message"])
        self.assertIn("ImportError: secret", event["details"])

    def test_emits_syntax_error(self):
        syntax_err = SyntaxError("unexpected EOF")
        syntax_err.filename = "my_app.py"
        syntax_err.lineno = 5

        with (
            patch.object(build_images_module, "load_code", side_effect=syntax_err),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name=None,
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("syntax error", event["message"])
        self.assertIn("my_app.py", event["message"])
        self.assertIn("5", event["message"])

    def test_emits_error_for_unhandled_load_exception(self):
        with (
            patch.object(
                build_images_module,
                "load_code",
                side_effect=RuntimeError("something unexpected"),
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name=None,
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("failed to load", event["message"])

    def test_emits_traceback_for_load_exception_when_debug_enabled(self):
        with (
            patch.dict(os.environ, {"TENSORLAKE_DEBUG": "1"}, clear=True),
            patch.object(
                build_images_module,
                "load_code",
                side_effect=RuntimeError("oops"),
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit):
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name=None,
                )

        event = emit.call_args.args[0]
        self.assertIn("traceback", event)
        self.assertIn("RuntimeError: oops", event["traceback"])

    def test_emits_error_when_no_images_in_application(self):
        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value={}),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name=None,
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("no images found", event["message"])

    def test_emits_image_definitions_for_all_images(self):
        infos = {
            "img1": _make_image_info(name="image-one", tag="v1"),
            "img2": _make_image_info(name="image-two", tag="v2"),
        }

        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value=infos),
            patch.object(
                build_images_module.importlib.metadata,
                "version",
                return_value="1.2.3",
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            build_images_module.build_images(
                application_file_path="my_app.py",
                tag=None,
                image_name=None,
            )

        emitted = [call.args[0] for call in emit.call_args_list]
        image_events = [e for e in emitted if e["type"] == "image"]
        self.assertEqual(len(image_events), 2)
        names = {e["name"] for e in image_events}
        self.assertEqual(names, {"image-one", "image-two"})
        self.assertEqual(emitted[-1]["type"], "done")

    def test_emits_image_definition_with_correct_fields(self):
        info = _make_image_info(name="my-image", tag="latest", base_image="python:3.12")
        infos = {"img": info}

        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value=infos),
            patch.object(
                build_images_module.importlib.metadata,
                "version",
                return_value="1.0.0",
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            build_images_module.build_images(
                application_file_path="my_app.py",
                tag=None,
                image_name=None,
            )

        image_event = next(
            e for e in (c.args[0] for c in emit.call_args_list) if e["type"] == "image"
        )
        self.assertEqual(image_event["name"], "my-image")
        self.assertEqual(image_event["tag"], "latest")
        self.assertEqual(image_event["base_image"], "python:3.12")
        self.assertEqual(image_event["sdk_version"], "1.0.0")
        self.assertIn("operations", image_event)

    def test_tag_override_replaces_image_tag(self):
        info = _make_image_info(name="my-image", tag="original-tag")
        infos = {"img": info}

        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value=infos),
            patch.object(
                build_images_module.importlib.metadata, "version", return_value="0.1.0"
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            build_images_module.build_images(
                application_file_path="my_app.py",
                tag="override-tag",
                image_name=None,
            )

        image_event = next(
            e for e in (c.args[0] for c in emit.call_args_list) if e["type"] == "image"
        )
        self.assertEqual(image_event["tag"], "override-tag")

    def test_image_name_filter_emits_only_matching_image(self):
        infos = {
            "img1": _make_image_info(name="wanted"),
            "img2": _make_image_info(name="unwanted"),
        }

        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value=infos),
            patch.object(
                build_images_module.importlib.metadata, "version", return_value="0.1.0"
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            build_images_module.build_images(
                application_file_path="my_app.py",
                tag=None,
                image_name="wanted",
            )

        image_events = [
            c.args[0] for c in emit.call_args_list if c.args[0]["type"] == "image"
        ]
        self.assertEqual(len(image_events), 1)
        self.assertEqual(image_events[0]["name"], "wanted")

    def test_image_name_filter_emits_error_when_no_match(self):
        infos = {"img": _make_image_info(name="other-image")}

        with (
            patch.object(build_images_module, "load_code"),
            patch.object(build_images_module, "image_infos", return_value=infos),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.build_images(
                    application_file_path="my_app.py",
                    tag=None,
                    image_name="nonexistent",
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("nonexistent", event["message"])

    def test_main_entrypoint_emits_error_for_unhandled_exception(self):
        with (
            patch("sys.argv", ["tensorlake-build-images", "my_app.py"]),
            patch.object(
                build_images_module,
                "build_images",
                side_effect=RuntimeError("unexpected"),
            ),
            patch.object(build_images_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                build_images_module.main()

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("build-images failed", event["message"])
        self.assertIn("RuntimeError: unexpected", event["details"])


if __name__ == "__main__":
    unittest.main()
