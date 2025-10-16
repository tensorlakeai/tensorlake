"""Tests for the tensorlake-python-analyzer tool."""

import json


def test_models_importable():
    """Test that all models can be imported."""
    from tensorlake.analyzer.models import (
        ApplicationConfigModel,
        ApplicationModel,
        AnalysisOutput,
        FunctionConfigModel,
        FunctionModel,
        ImageBuildOperationModel,
        ImageModel,
        RetriesModel,
    )

    assert ApplicationConfigModel is not None
    assert ApplicationModel is not None
    assert AnalysisOutput is not None
    assert FunctionConfigModel is not None
    assert FunctionModel is not None
    assert ImageBuildOperationModel is not None
    assert ImageModel is not None
    assert RetriesModel is not None


def test_retries_model_creation():
    """Test that RetriesModel can be created with default values."""
    from tensorlake.analyzer.models import RetriesModel

    retries = RetriesModel()
    assert retries.max_retries == 0
    assert retries.initial_delay == 1.0
    assert retries.max_delay == 60.0
    assert retries.delay_multiplier == 2.0

    # Test with custom values
    custom_retries = RetriesModel(
        max_retries=3,
        initial_delay=2.0,
        max_delay=120.0,
        delay_multiplier=3.0,
    )
    assert custom_retries.max_retries == 3
    assert custom_retries.initial_delay == 2.0


def test_image_model_creation():
    """Test that ImageModel can be created."""
    from tensorlake.analyzer.models import ImageBuildOperationModel, ImageModel

    image = ImageModel(
        name="test-image",
        tag="v1.0",
        base_image="python:3.10-slim",
        build_operations=[
            ImageBuildOperationModel(
                type="RUN",
                args=["pip install numpy"],
                options={},
            )
        ],
    )

    assert image.name == "test-image"
    assert image.tag == "v1.0"
    assert image.base_image == "python:3.10-slim"
    assert len(image.build_operations) == 1
    assert image.build_operations[0].type == "RUN"


def test_function_config_model_creation():
    """Test that FunctionConfigModel can be created."""
    from tensorlake.analyzer.models import FunctionConfigModel, RetriesModel

    func_config = FunctionConfigModel(
        function_name="test_function",
        description="Test function",
        image_name="test-image",
        secrets=["SECRET1", "SECRET2"],
        retries=RetriesModel(max_retries=3),
        timeout=300,
        cpu=2.0,
        memory=4.0,
        ephemeral_disk=10.0,
        cacheable=True,
        max_concurrency=5,
    )

    assert func_config.function_name == "test_function"
    assert func_config.description == "Test function"
    assert func_config.image_name == "test-image"
    assert len(func_config.secrets) == 2
    assert func_config.retries.max_retries == 3
    assert func_config.cpu == 2.0
    assert func_config.cacheable is True


def test_analysis_output_serialization():
    """Test that AnalysisOutput can be serialized to JSON."""
    from tensorlake.analyzer.models import (
        AnalysisOutput,
        ApplicationConfigModel,
        ApplicationModel,
        FunctionConfigModel,
        FunctionModel,
        ImageModel,
        RetriesModel,
    )

    output = AnalysisOutput(
        images={
            "test-image": ImageModel(
                name="test-image",
                tag="latest",
                base_image="python:3.10",
                build_operations=[],
            )
        },
        functions={
            "test_func": FunctionModel(
                function_name="test_func",
                function_config=FunctionConfigModel(
                    function_name="test_func",
                    description="Test",
                    image_name="test-image",
                    secrets=[],
                    timeout=300,
                    cpu=1.0,
                    memory=1.0,
                    ephemeral_disk=2.0,
                    cacheable=False,
                    max_concurrency=1,
                ),
                application_config=ApplicationConfigModel(
                    tags={"env": "test"},
                    retries=RetriesModel(),
                    input_serializer="json",
                    output_serializer="json",
                    version="v1",
                ),
            )
        },
        applications={
            "test_app": ApplicationModel(
                application_name="test_app",
                version="v1",
                functions=["test_func"],
                config=ApplicationConfigModel(
                    tags={"env": "test"},
                    retries=RetriesModel(),
                    input_serializer="json",
                    output_serializer="json",
                    version="v1",
                ),
            )
        },
    )

    # Test dict conversion
    result_dict = output.to_dict()
    assert result_dict is not None

    # Test that it's valid JSON
    json_str = json.dumps(result_dict)
    parsed = json.loads(json_str)
    assert "images" in parsed
    assert "functions" in parsed
    assert "applications" in parsed
    assert "test-image" in parsed["images"]
    assert "test_func" in parsed["functions"]
    assert "test_app" in parsed["applications"]


def test_existing_retries_class_compatibility():
    """Test that the original Retries class still works with Pydantic."""
    from tensorlake.applications.interface.retries import Retries

    # Test default instantiation
    retries = Retries()
    assert retries.max_retries == 0
    assert retries.initial_delay == 1.0

    # Test with custom values
    custom_retries = Retries(max_retries=5)
    assert custom_retries.max_retries == 5

    # Test that it has Pydantic methods
    assert hasattr(retries, "model_dump")
    assert hasattr(retries, "model_dump_json")


def test_function_configuration_compatibility():
    """Test that _FunctionConfiguration works as a Pydantic model."""
    from tensorlake.applications.interface.function import _FunctionConfiguration
    from tensorlake.applications.interface.image import Image
    from tensorlake.applications.interface.retries import Retries

    img = Image(name="test")

    config = _FunctionConfiguration(
        function_name="test",
        description="Test function",
        image=img,
        secrets=["SECRET1"],
        timeout=300,
        cpu=1.0,
        memory=1.0,
        ephemeral_disk=2.0,
        cacheable=False,
        max_concurrency=1,
    )

    assert config.function_name == "test"
    assert config.image.name == "test"
    assert len(config.secrets) == 1

    # Test that attributes can be mutated (required for decorators.py)
    config.class_name = "TestClass"
    config.class_method_name = "test_method"
    assert config.class_name == "TestClass"
    assert config.class_method_name == "test_method"

    # Test Pydantic features
    assert hasattr(config, "model_dump")


def test_application_configuration_compatibility():
    """Test that _ApplicationConfiguration works as a Pydantic model."""
    from tensorlake.applications.interface.function import _ApplicationConfiguration
    from tensorlake.applications.interface.retries import Retries

    config = _ApplicationConfiguration(
        tags={"env": "test"},
        retries=Retries(max_retries=3),
        input_serializer="json",
        output_serializer="json",
        version="v1.0",
    )

    assert config.version == "v1.0"
    assert config.tags["env"] == "test"
    assert config.retries.max_retries == 3

    # Test Pydantic features
    assert hasattr(config, "model_dump")
