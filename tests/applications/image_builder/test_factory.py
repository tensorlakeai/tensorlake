"""Tests for ImageBuilder factory and version detection."""

import os
from unittest.mock import MagicMock, patch

import pytest

from tensorlake.applications.image_builder.exceptions import ImageBuilderConfigError
from tensorlake.applications.image_builder.factory import (
    create_image_builder_from_context,
    get_image_builder_version,
)


class TestVersionDetection:
    """Tests for version detection logic."""

    def test_version_detection_default(self):
        """Test that default version is v2."""
        with patch.dict(os.environ, {}, clear=True):
            version = get_image_builder_version()
            assert version == "v2"

    def test_version_detection_env_var(self):
        """Test version detection from environment variable."""
        with patch.dict(os.environ, {"TENSORLAKE_IMAGE_BUILDER_VERSION": "v3"}):
            version = get_image_builder_version()
            assert version == "v3"

    def test_version_detection_cli_override(self):
        """Test that CLI flag overrides environment variable."""
        with patch.dict(os.environ, {"TENSORLAKE_IMAGE_BUILDER_VERSION": "v2"}):
            version = get_image_builder_version(override="v3")
            assert version == "v3"

    def test_version_detection_shorthand_v2(self):
        """Test shorthand '2' normalizes to 'v2'."""
        version = get_image_builder_version(override="2")
        assert version == "v2"

    def test_version_detection_shorthand_v3(self):
        """Test shorthand '3' normalizes to 'v3'."""
        version = get_image_builder_version(override="3")
        assert version == "v3"

    def test_version_detection_invalid(self):
        """Test that invalid version raises error."""
        with pytest.raises(ImageBuilderConfigError) as exc_info:
            get_image_builder_version(override="v99")
        assert "Invalid image builder version" in str(exc_info.value)
        assert "v99" in str(exc_info.value)

    def test_version_detection_priority_order(self):
        """Test priority order: CLI > env > default."""
        # Default
        with patch.dict(os.environ, {}, clear=True):
            assert get_image_builder_version() == "v2"

        # Env var
        with patch.dict(os.environ, {"TENSORLAKE_IMAGE_BUILDER_VERSION": "v3"}):
            assert get_image_builder_version() == "v3"

        # CLI override wins
        with patch.dict(os.environ, {"TENSORLAKE_IMAGE_BUILDER_VERSION": "v2"}):
            assert get_image_builder_version(override="v3") == "v3"

    def test_version_detection_case_insensitive(self):
        """Test version detection is case insensitive."""
        assert get_image_builder_version(override="V2") == "v2"
        assert get_image_builder_version(override="V3") == "v3"

    def test_version_detection_whitespace(self):
        """Test version detection handles whitespace."""
        assert get_image_builder_version(override=" v2 ") == "v2"
        assert get_image_builder_version(override=" v3 ") == "v3"


class TestFactory:
    """Tests for builder factory."""

    def test_create_v2_builder(self):
        """Test factory returns v2 adapter."""
        with patch.dict(os.environ, {}, clear=True):
            builder = create_image_builder_from_context(
                api_key="test-key",
                organization_id="test-org",
                project_id="test-proj",
                version="v2",
            )
            # Check that it's the v2 adapter
            from tensorlake.applications.image_builder.adapter_v2 import (
                ImageBuilderV2Adapter,
            )

            assert isinstance(builder, ImageBuilderV2Adapter)

    def test_create_v3_builder(self):
        """Test factory returns v3 builder."""
        builder = create_image_builder_from_context(
            api_key="test-key",
            organization_id="test-org",
            project_id="test-proj",
            version="v3",
        )
        # Check that it's the v3 builder
        from tensorlake.applications.image_builder import ImageBuilder

        assert isinstance(builder, ImageBuilder)

    def test_create_builder_auto_detect(self):
        """Test factory auto-detects version."""
        with patch.dict(os.environ, {"TENSORLAKE_IMAGE_BUILDER_VERSION": "v3"}):
            builder = create_image_builder_from_context(
                api_key="test-key",
                organization_id="test-org",
                project_id="test-proj",
            )
            from tensorlake.applications.image_builder import ImageBuilder

            assert isinstance(builder, ImageBuilder)

    def test_create_builder_with_pat(self):
        """Test factory works with PAT instead of API key."""
        builder = create_image_builder_from_context(
            pat="test-pat",
            organization_id="test-org",
            project_id="test-proj",
            version="v2",
        )
        from tensorlake.applications.image_builder.adapter_v2 import (
            ImageBuilderV2Adapter,
        )

        assert isinstance(builder, ImageBuilderV2Adapter)
