import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

from tensorlake.applications import Function, Image
from tensorlake.applications.image import create_image_context_file


@dataclass(frozen=True)
class ApplicationBuildImageRequest:
    key: str
    name: str
    context_sha256: str
    function_names: list[str]
    context_tar_gz: bytes


@dataclass(frozen=True)
class ApplicationBuildRequest:
    name: str
    version: str
    images: list[ApplicationBuildImageRequest]


def collect_application_build_request(
    application: Function, functions: list[Function]
) -> ApplicationBuildRequest:
    image_requests: dict[Image, ApplicationBuildImageRequest] = {}
    image_functions: dict[Image, list[str]] = {}

    for function in _functions_for_application(application, functions):
        image = function._function_config.image
        if image not in image_requests:
            context_tar_gz = build_image_context(image)
            image_requests[image] = ApplicationBuildImageRequest(
                key=image._id,
                name=image.name,
                context_sha256=hashlib.sha256(context_tar_gz).hexdigest(),
                function_names=[],
                context_tar_gz=context_tar_gz,
            )
            image_functions[image] = image_requests[image].function_names

        image_functions[image].append(function._function_config.function_name)

    return ApplicationBuildRequest(
        name=application._function_config.function_name,
        version=application._application_config.version,
        images=list(image_requests.values()),
    )


def _functions_for_application(
    application: Function, functions: list[Function]
) -> list[Function]:
    return [
        function
        for function in functions
        if function is application or function._application_config is None
    ]


def build_image_context(image: Image) -> bytes:
    fd, context_file_path = tempfile.mkstemp()
    os.close(fd)

    try:
        create_image_context_file(image, context_file_path)
        return Path(context_file_path).read_bytes()
    finally:
        if os.path.exists(context_file_path):
            os.remove(context_file_path)
