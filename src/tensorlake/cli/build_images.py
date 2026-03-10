import argparse
import asyncio
import importlib.metadata
import json
import os
import sys
import traceback

from tensorlake.applications.image import ImageInformation, image_infos
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.cli import deploy as deploy_module


def _emit(obj):
    print(json.dumps(obj), flush=True)


def _debug_enabled() -> bool:
    return os.environ.get("TENSORLAKE_DEBUG", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def build_images(
    application_file_path: str,
    tag: str | None,
    image_name: str | None,
):
    """Load application file and emit image definitions as NDJSON."""
    try:
        application_file_path = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except SyntaxError as e:
        _emit(
            {
                "type": "error",
                "message": f"syntax error in {e.filename}, line {e.lineno}: {e.msg}",
            }
        )
        sys.exit(1)
    except ImportError as e:
        _emit(
            {
                "type": "error",
                "message": "failed to import application file. make sure all dependencies are installed in your current environment.",
                "details": f"{type(e).__name__}: {e}",
            }
        )
        sys.exit(1)
    except Exception as e:
        event = {
            "type": "error",
            "message": f"failed to load {application_file_path}",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)

    infos: dict = image_infos()

    if not infos:
        _emit({"type": "error", "message": "no images found in application file"})
        sys.exit(1)

    sdk_version = importlib.metadata.version("tensorlake")

    emitted = 0
    for info in infos.values():
        info: ImageInformation
        image = info.image

        if image_name and image.name != image_name:
            continue

        effective_tag = tag or image.tag

        _emit(
            {
                "type": "image",
                "name": image.name,
                "tag": effective_tag,
                "base_image": image._base_image,
                "sdk_version": sdk_version,
                "operations": [
                    {
                        "op": op.type.name,
                        "args": op.args,
                        "options": op.options,
                    }
                    for op in image._build_operations
                ],
            }
        )
        emitted += 1

    if emitted == 0:
        _emit(
            {
                "type": "error",
                "message": (
                    f"no image named '{image_name}' found in application file"
                    if image_name
                    else "no images found"
                ),
            }
        )
        sys.exit(1)

    _emit({"type": "done"})


def build_images_with_builder(
    application_file_path: str,
    image_builder_version: str,
):
    """Load application file and build images via the configured remote image builder."""
    try:
        application_file_path = os.path.abspath(application_file_path)
        load_code(application_file_path)
    except SyntaxError as e:
        _emit(
            {
                "type": "error",
                "message": f"syntax error in {e.filename}, line {e.lineno}: {e.msg}",
            }
        )
        sys.exit(1)
    except ImportError as e:
        _emit(
            {
                "type": "error",
                "message": "failed to import application file. make sure all dependencies are installed in your current environment.",
                "details": f"{type(e).__name__}: {e}",
            }
        )
        sys.exit(1)
    except Exception as e:
        event = {
            "type": "error",
            "message": f"failed to load {application_file_path}",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)

    auth = deploy_module._build_context_from_env()
    functions = get_functions()
    builder = deploy_module.mk_builder(image_builder_version, auth)

    try:
        asyncio.run(deploy_module._prepare_images(builder, functions))
    except KeyboardInterrupt:
        _emit({"type": "error", "message": "build cancelled by user"})
        sys.exit(1)
    except Exception as e:
        event = {
            "type": "error",
            "message": f"build-images failed ({type(e).__name__})",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Emit image definitions for images defined in a Tensorlake application file"
    )
    parser.add_argument(
        "application_file_path",
        help="Path to the application .py file",
    )
    parser.add_argument(
        "--tag",
        "-t",
        default=None,
        help="Tag to use for the images (overrides the tag defined in the image)",
    )
    parser.add_argument(
        "--image-name",
        "-i",
        default=None,
        help="Build only the image with this name",
    )
    parser.add_argument(
        "--image-builder-version",
        choices=["v2", "v3"],
        default=None,
        help="Build images through the remote image builder instead of emitting definitions",
    )
    args = parser.parse_args()

    try:
        if args.image_builder_version is None:
            build_images(
                application_file_path=args.application_file_path,
                tag=args.tag,
                image_name=args.image_name,
            )
        else:
            build_images_with_builder(
                application_file_path=args.application_file_path,
                image_builder_version=args.image_builder_version,
            )
    except SystemExit:
        raise
    except Exception as e:
        event = {
            "type": "error",
            "message": f"build-images failed ({type(e).__name__})",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)


if __name__ == "__main__":
    main()
