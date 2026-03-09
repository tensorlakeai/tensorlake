import unittest
from io import StringIO
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.client_v2 import (
    ApplicationImageBuildError,
    ImageBuilderV2Client,
)


class TestImageBuilderV2Client(unittest.IsolatedAsyncioTestCase):
    async def test_build_preserves_sequential_per_function_behavior(self):
        starts: list[tuple[str, str]] = []
        builder = ImageBuilderV2Client(
            cloud_client=MagicMock(),
            on_build_start=lambda image, function_name: starts.append(
                (image.name, function_name)
            ),
        )
        builder._build_single = AsyncMock()

        request = ApplicationBuildRequest(
            name="app_fn",
            version="v1",
            images=[
                ApplicationBuildImageRequest(
                    key="img-1",
                    name="image-a",
                    context_sha256="sha-a",
                    function_names=["fn-1", "fn-2"],
                    context_tar_gz=b"context-a",
                ),
                ApplicationBuildImageRequest(
                    key="img-2",
                    name="image-b",
                    context_sha256="sha-b",
                    function_names=["fn-3"],
                    context_tar_gz=b"context-b",
                ),
            ],
        )

        stderr = StringIO()
        with patch("sys.stderr", stderr):
            await builder.build(request)

        self.assertEqual(
            starts,
            [("image-a", "fn-1"), ("image-a", "fn-2"), ("image-b", "fn-3")],
        )
        self.assertEqual(builder._build_single.await_count, 3)
        self.assertEqual(
            [
                call.kwargs["function_name"]
                for call in builder._build_single.await_args_list
            ],
            ["fn-1", "fn-2", "fn-3"],
        )
        self.assertEqual(
            stderr.getvalue().splitlines(),
            [
                "Building images...",
                "Building image-a",
                "Built image-a with context sha256 sha-a",
                "Building image-a",
                "Built image-a with context sha256 sha-a",
                "Building image-b",
                "Built image-b with context sha256 sha-b",
            ],
        )

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
