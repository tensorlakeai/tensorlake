"""Tests for backward compatibility."""

import os
import warnings
from unittest.mock import patch

import pytest

from tensorlake.applications.image_builder.factory import get_image_builder_version


class TestBackwardCompatibility:
    """Tests for backward compatibility."""

    def test_old_v2_import_path_with_warning(self):
        """Test old import path still works but warns."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            # Import from old location
            from tensorlake.builder.client_v2 import (
                BuildContext,
                ImageBuilderV2Client,
            )

            # Check that a deprecation warning was issued
            assert len(w) >= 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "deprecated" in str(w[0].message).lower()
            assert "tensorlake.applications.image_builder.client_v2" in str(w[0].message)

            # Check that imports work
            assert BuildContext is not None
            assert ImageBuilderV2Client is not None

    def test_default_is_v2(self):
        """Test that default version is v2 for backward compatibility."""
        with patch.dict(os.environ, {}, clear=True):
            version = get_image_builder_version()
            assert version == "v2"

    def test_v2_adapter_matches_old_behavior(self):
        """Test v2 adapter provides same interface as old behavior."""
        from tensorlake.applications.image_builder.adapter_v2 import (
            ImageBuilderV2Adapter,
        )

        # Create adapter
        adapter = ImageBuilderV2Adapter.from_context(
            api_key="test-key",
            organization_id="test-org",
            project_id="test-proj",
        )

        # Check it has the build method
        assert hasattr(adapter, "build")
        assert callable(adapter.build)

    def test_old_exception_handling(self):
        """Test that old exception handling still works."""
        from tensorlake.applications.image_builder.exceptions import (
            ImageBuilderError,
            ImageBuilderV2Error,
        )

        # Old code catching base exception should still work
        try:
            raise ImageBuilderV2Error("test error")
        except ImageBuilderError as e:
            # Should catch successfully
            assert "test error" in str(e)
