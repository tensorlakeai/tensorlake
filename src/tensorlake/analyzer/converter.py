"""Convert internal Tensorlake objects to analyzer models."""

import inspect
import os
from typing import Dict, List, Optional

from tensorlake.applications.image import ImageInformation, image_infos
from tensorlake.applications.interface.function import (
    _ApplicationConfiguration,
    _FunctionConfiguration,
)
from tensorlake.applications.registry import get_functions

from .models import (
    ApplicationConfigModel,
    ApplicationModel,
    CodeZIPManifest,
    FunctionConfigModel,
    FunctionModel,
    FunctionZIPManifest,
    ImageBuildOperationModel,
    ImageModel,
    RetriesModel,
)


def convert_image_to_model(image) -> ImageModel:
    """Convert Image object to model."""
    build_ops = []
    for op in image._build_operations:
        build_ops.append(
            ImageBuildOperationModel(
                type=op.type.name,
                args=op.args,
                options=op.options if hasattr(op, "options") else {},
            )
        )

    return ImageModel(
        name=image.name,
        tag=image.tag,
        base_image=image._base_image,
        build_operations=build_ops,
    )


def convert_retries_to_model(retries) -> Optional[RetriesModel]:
    """Convert Retries object to model."""
    if retries is None:
        return None

    return RetriesModel(
        max_retries=retries.max_retries,
        initial_delay=retries.initial_delay,
        max_delay=retries.max_delay,
        delay_multiplier=retries.delay_multiplier,
    )


def convert_function_config_to_model(
    fn_config: _FunctionConfiguration,
) -> FunctionConfigModel:
    """Convert _FunctionConfiguration to model."""
    return FunctionConfigModel(
        class_name=fn_config.class_name,
        class_method_name=fn_config.class_method_name,
        class_init_timeout=fn_config.class_init_timeout,
        function_name=fn_config.function_name,
        description=fn_config.description,
        image_name=fn_config.image.name,
        secrets=fn_config.secrets if fn_config.secrets else [],
        retries=convert_retries_to_model(fn_config.retries),
        timeout=fn_config.timeout,
        cpu=fn_config.cpu,
        memory=fn_config.memory,
        ephemeral_disk=fn_config.ephemeral_disk,
        gpu=fn_config.gpu,
        region=fn_config.region,
        cacheable=fn_config.cacheable,
        max_concurrency=fn_config.max_concurrency,
    )


def convert_application_config_to_model(
    app_config: _ApplicationConfiguration,
) -> ApplicationConfigModel:
    """Convert _ApplicationConfiguration to model."""
    return ApplicationConfigModel(
        tags=app_config.tags if app_config.tags else {},
        retries=convert_retries_to_model(app_config.retries),
        region=app_config.region,
        input_serializer=app_config.input_serializer,
        output_serializer=app_config.output_serializer,
        version=app_config.version,
    )


def extract_images() -> Dict[str, ImageModel]:
    """Extract all images from registered functions."""
    images_dict: Dict[str, ImageModel] = {}
    image_info_dict: Dict = image_infos()

    for image_info in image_info_dict.values():
        image_info: ImageInformation
        image_model = convert_image_to_model(image_info.image)
        images_dict[image_model.name] = image_model

    return images_dict


def create_function_zip_manifest(
    function, code_dir_path: str
) -> Optional[FunctionZIPManifest]:
    """Create ZIP manifest entry for a function."""
    try:
        function_name: str = function.function_config.function_name
        import_file_path: str = inspect.getsourcefile(function.original_function)

        if import_file_path is None:
            return None

        import_file_path = os.path.abspath(import_file_path)

        if not import_file_path.startswith(code_dir_path):
            return None

        import_file_path_in_code_dir: str = os.path.relpath(
            import_file_path, start=code_dir_path
        )

        # Converts relative path "foo/bar/buzz.py" to "foo.bar.buzz"
        module_import_name: str = os.path.splitext(import_file_path_in_code_dir)[
            0
        ].replace(os.sep, ".")

        return FunctionZIPManifest(
            name=function_name,
            module_import_name=module_import_name,
        )
    except Exception:
        return None


def extract_functions_and_applications(
    code_dir_path: str,
) -> tuple[Dict[str, FunctionModel], Dict[str, ApplicationModel], CodeZIPManifest]:
    """Extract ALL functions, applications, and code manifest from registered functions.

    This includes:
    - Functions with @function() only (no application config)
    - Functions with both @function() and @application() (have application config)
    """
    functions_dict: Dict[str, FunctionModel] = {}
    applications_dict: Dict[str, ApplicationModel] = {}
    code_manifest = CodeZIPManifest()

    functions: List = get_functions()

    # Process ALL functions, not just those with applications
    for function in functions:
        # Skip if the function doesn't have a function_config
        if not hasattr(function, "function_config") or function.function_config is None:
            continue

        fn_config: _FunctionConfiguration = function.function_config
        app_config: Optional[_ApplicationConfiguration] = function.application_config

        # Add function (all functions, whether they have app config or not)
        function_model = FunctionModel(
            function_name=fn_config.function_name,
            function_config=convert_function_config_to_model(fn_config),
            application_config=(
                convert_application_config_to_model(app_config) if app_config else None
            ),
        )
        functions_dict[fn_config.function_name] = function_model

        # Create ZIP manifest entry for this function
        zip_manifest = create_function_zip_manifest(function, code_dir_path)
        if zip_manifest:
            code_manifest.functions[fn_config.function_name] = zip_manifest

        # Only create application entries for functions that have application config
        if app_config is not None:
            app_name = fn_config.function_name

            if app_name not in applications_dict:
                applications_dict[app_name] = ApplicationModel(
                    application_name=app_name,
                    version=app_config.version,
                    functions=[fn_config.function_name],
                    config=convert_application_config_to_model(app_config),
                )
            else:
                if fn_config.function_name not in applications_dict[app_name].functions:
                    applications_dict[app_name].functions.append(
                        fn_config.function_name
                    )

    return functions_dict, applications_dict, code_manifest
