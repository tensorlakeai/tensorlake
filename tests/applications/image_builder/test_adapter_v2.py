"""Tests for ImageBuilderV2Adapter."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.applications import Image
from tensorlake.applications.image import ImageInformation
from tensorlake.applications.image_builder import BuildRequest, ImageBuildRequest
from tensorlake.applications.image_builder.adapter_v2 import ImageBuilderV2Adapter
from tensorlake.applications.image_builder.client_v2 import BuildInfo
from tensorlake.applications.image_builder.exceptions import (
    ImageBuilderV2BuildError,
    ImageBuilderV2NetworkError,
)


@pytest.fixture
def mock_v2_client():
    """Create a mock v2 client."""
    client = MagicMock()
    client.build = AsyncMock()
    return client


@pytest.fixture
def sample_build_request():
    """Create a sample BuildRequest for testing."""
    req = BuildRequest(name="test-app", version="1.0.0")

    # Create a mock image and image info
    image = MagicMock(spec=Image)
    image.name = "test-image"

    # Create mock function info
    func_info = MagicMock()
    func_info.function_name = "test-function"

    image_info = MagicMock(spec=ImageInformation)
    image_info.image = image
    image_info.functions = [func_info]

    req.add_image(image_info)

    return req


class TestAdapterConstruction:
    """Tests for adapter construction."""

    def test_adapter_init(self, mock_v2_client):
        """Test adapter initialization."""
        adapter = ImageBuilderV2Adapter(client=mock_v2_client)
        assert adapter._client == mock_v2_client

    def test_adapter_from_context(self):
        """Test adapter creation from context."""
        adapter = ImageBuilderV2Adapter.from_context(
            api_key="test-key",
            organization_id="test-org",
            project_id="test-proj",
        )
        assert adapter is not None
        assert adapter._client is not None


class TestAdapterBuild:
    """Tests for adapter build method."""

    @pytest.mark.asyncio
    async def test_build_success(self, mock_v2_client, sample_build_request):
        """Test successful build."""
        # Mock successful build
        build_info = BuildInfo(
            id="build-123",
            status="succeeded",
            created_at="2024-01-01T00:00:00Z",
            updated_at="2024-01-01T00:01:00Z",
            finished_at="2024-01-01T00:01:00Z",
            error_message=None,
        )
        mock_v2_client.build.return_value = build_info

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)

        # Should not raise
        await adapter.build(sample_build_request)

        # Verify build was called
        assert mock_v2_client.build.called

    @pytest.mark.asyncio
    async def test_build_failure_runtime_error(self, mock_v2_client, sample_build_request):
        """Test build failure with RuntimeError."""
        # Mock build failure
        mock_v2_client.build.side_effect = RuntimeError("Build failed")

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)

        # Should raise wrapped exception
        with pytest.raises(ImageBuilderV2BuildError) as exc_info:
            await adapter.build(sample_build_request)

        assert "Build failed" in str(exc_info.value)
        assert exc_info.value.version == "v2"

    @pytest.mark.asyncio
    async def test_build_network_error(self, mock_v2_client, sample_build_request):
        """Test build failure with network error."""
        # Mock network error
        mock_v2_client.build.side_effect = Exception("Connection error: network timeout")

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)

        # Should raise network error
        with pytest.raises(ImageBuilderV2NetworkError) as exc_info:
            await adapter.build(sample_build_request)

        assert "network" in str(exc_info.value).lower()
        assert exc_info.value.version == "v2"

    @pytest.mark.asyncio
    async def test_build_generic_error(self, mock_v2_client, sample_build_request):
        """Test build failure with generic error."""
        # Mock generic error
        mock_v2_client.build.side_effect = ValueError("Invalid input")

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)

        # Should raise build error
        with pytest.raises(ImageBuilderV2BuildError) as exc_info:
            await adapter.build(sample_build_request)

        assert "Invalid input" in str(exc_info.value)
        assert exc_info.value.version == "v2"


class TestAdapterSequentialBehavior:
    """Tests for sequential building behavior."""

    @pytest.mark.asyncio
    async def test_sequential_order(self, mock_v2_client):
        """Test images are built sequentially in order."""
        # Create request with multiple images
        req = BuildRequest(name="test-app", version="1.0.0")

        build_order = []

        def track_build(*args, **kwargs):
            image = args[1]
            build_order.append(image.name)
            return AsyncMock(return_value=BuildInfo(
                id=f"build-{image.name}",
                status="succeeded",
                created_at="2024-01-01T00:00:00Z",
                updated_at="2024-01-01T00:01:00Z",
                finished_at="2024-01-01T00:01:00Z",
                error_message=None,
            ))()

        mock_v2_client.build.side_effect = track_build

        # Add multiple images
        for i in range(3):
            image = MagicMock(spec=Image)
            image.name = f"image-{i}"

            func_info = MagicMock()
            func_info.function_name = f"func-{i}"

            image_info = MagicMock(spec=ImageInformation)
            image_info.image = image
            image_info.functions = [func_info]

            req.add_image(image_info)

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)
        await adapter.build(req)

        # Verify sequential order
        assert build_order == ["image-0", "image-1", "image-2"]


class TestAdapterExceptionWrapping:
    """Tests for exception wrapping."""

    @pytest.mark.asyncio
    async def test_exception_includes_version(self, mock_v2_client, sample_build_request):
        """Test wrapped exceptions include version."""
        mock_v2_client.build.side_effect = RuntimeError("Error")

        adapter = ImageBuilderV2Adapter(client=mock_v2_client)

        with pytest.raises(ImageBuilderV2BuildError) as exc_info:
            await adapter.build(sample_build_request)

        assert exc_info.value.version == "v2"
        assert "v2" in str(exc_info.value)


class TestAdapterAuthContext:
    """Tests for authentication context handling."""

    def test_from_context_with_api_key(self):
        """Test from_context with API key."""
        adapter = ImageBuilderV2Adapter.from_context(
            api_key="test-key",
            organization_id="test-org",
            project_id="test-proj",
        )
        assert adapter is not None

    def test_from_context_with_pat(self):
        """Test from_context with PAT."""
        adapter = ImageBuilderV2Adapter.from_context(
            pat="test-pat",
            organization_id="test-org",
            project_id="test-proj",
        )
        assert adapter is not None
