import hashlib
import re
from dataclasses import dataclass

from tensorlake.image.sandbox_builder import build_sandbox_application_image

from ..applications import filter_applications, functions_for_application
from ..image import Image
from ..interface.exceptions import SDKUsageError
from ..interface.function import Function

_DEFAULT_APPLICATION_IMAGE_NAME = "default"
_IMAGE_NAME_COMPONENT = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*\Z")


@dataclass(frozen=True)
class ApplicationImageBuild:
    image: Image
    registered_name: str
    function_keys: tuple[tuple[str, str], ...]


def registered_image_component(value: str) -> str:
    if len(value) <= 64 and _IMAGE_NAME_COMPONENT.fullmatch(value):
        return value
    return f"id-{hashlib.sha256(value.encode('utf-8')).hexdigest()[:32]}"


def default_application_image_name(
    application_name: str, application_version: str
) -> str:
    return (
        f"applications/{registered_image_component(application_name)}"
        f"/versions/{registered_image_component(application_version)}/default"
    )


def explicit_application_image_name(
    application_name: str, application_version: str, image_name: str
) -> str:
    return (
        f"applications/{registered_image_component(application_name)}"
        f"/versions/{registered_image_component(application_version)}"
        f"/images/{registered_image_component(image_name)}"
    )


def _append_image_build(
    image_builds: list[ApplicationImageBuild],
    seen_builds: set[tuple[str, str]],
    seen_registered_names: dict[str, str],
    image: Image,
    registered_name: str,
    function_key: tuple[str, str],
) -> None:
    previous_image_id = seen_registered_names.get(registered_name)
    if previous_image_id is not None and previous_image_id != image._id:
        raise SDKUsageError(
            f"multiple different Image objects use the name '{registered_name}'. "
            "Use unique Image(name=...) values so each function resolves "
            "to the intended sandbox image."
        )
    seen_registered_names[registered_name] = image._id
    build_key = (image._id, registered_name)
    if build_key in seen_builds:
        for index, image_build in enumerate(image_builds):
            if (image_build.image._id, image_build.registered_name) == build_key:
                image_builds[index] = ApplicationImageBuild(
                    image=image_build.image,
                    registered_name=image_build.registered_name,
                    function_keys=(*image_build.function_keys, function_key),
                )
                return
        raise AssertionError("recorded application image build is missing")
    seen_builds.add(build_key)
    image_builds.append(
        ApplicationImageBuild(
            image=image,
            registered_name=registered_name,
            function_keys=(function_key,),
        )
    )


def application_image_builds(functions: list[Function]) -> list[ApplicationImageBuild]:
    image_builds: list[ApplicationImageBuild] = []
    for application in filter_applications(functions):
        seen_builds: set[tuple[str, str]] = set()
        seen_image_names: dict[str, str] = {}
        application_name = application._function_config.function_name
        application_version = application._application_config.version
        default_image: Image | None = None
        default_registered_name = default_application_image_name(
            application_name,
            application_version,
        )
        for function in functions_for_application(application, functions):
            function_key = (
                application_name,
                function._function_config.function_name,
            )
            image = function._function_config.image
            if image is None:
                if default_image is None:
                    default_image = Image(name=_DEFAULT_APPLICATION_IMAGE_NAME)
                _append_image_build(
                    image_builds,
                    seen_builds,
                    seen_image_names,
                    default_image,
                    default_registered_name,
                    function_key,
                )
                continue
            _append_image_build(
                image_builds,
                seen_builds,
                seen_image_names,
                image,
                explicit_application_image_name(
                    application_name, application_version, image.name
                ),
                function_key,
            )
    return image_builds


def prepare_application_images(
    functions: list[Function],
    *,
    context_dir: str,
    build_envs: list[tuple[str, str]] | None = None,
) -> dict[tuple[str, str], str]:
    function_images: dict[tuple[str, str], str] = {}
    for image_build in application_image_builds(functions):
        build_sandbox_application_image(
            image_build.image,
            registered_name=image_build.registered_name,
            build_env_vars=build_envs,
            context_dir=context_dir,
        )
        for function_key in image_build.function_keys:
            function_images[function_key] = image_build.registered_name
    return function_images
