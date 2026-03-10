import unittest
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.client_v2 import (
    ApplicationImageBuildError,
    ImageBuilderV2Client,
)


class TestImageBuilderV2Client(unittest.IsolatedAsyncioTestCase):
    async def test_build_wraps_failures_with_image_name(self):
        builder = ImageBuilderV2Client(cloud_client=MagicMock())
        builder._build_single = AsyncMock(side_effect=RuntimeError("boom"))

        request = ApplicationBuildRequest(
            name="app_fn",
            version="v1",
            images=[
                ApplicationBuildImageRequest(
                    key="img-1",
                    name="image-a",
                    context_sha256="sha-a",
                    function_names=["fn-1"],
                    context_tar_gz=b"context-a",
                )
            ],
        )

        with self.assertRaises(ApplicationImageBuildError) as exc:
            await builder.build(request)

        self.assertEqual(exc.exception.image_name, "image-a")
        self.assertEqual(str(exc.exception.error), "boom")
