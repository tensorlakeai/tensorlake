import asyncio
import json
import unittest
from unittest.mock import MagicMock

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.client_v3 import ImageBuilderV3Client


class TestImageBuilderV3Client(unittest.IsolatedAsyncioTestCase):
    async def test_build_translates_request_with_image_contexts_and_parses_response(
        self,
    ):
        cloud_client = MagicMock()
        cloud_client.create_application_build.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "building",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "app_version_id": "app-version-1",
                        "key": "img-1",
                        "name": "image-a",
                        "description": "Image A",
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:01:00Z",
                        "function_names": ["fn-1", "fn-2"],
                        "status": "pending",
                    }
                ],
            }
        )
        cloud_client.application_build_info_json.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "succeeded",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "app_version_id": "app-version-1",
                        "key": "img-1",
                        "name": "image-a",
                        "description": "Image A",
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:02:00Z",
                        "finished_at": "2026-03-07T10:03:00Z",
                        "function_names": ["fn-1", "fn-2"],
                        "status": "succeeded",
                    },
                    {
                        "id": "img-build-2",
                        "app_version_id": "app-version-1",
                        "key": "img-2",
                        "name": "image-b",
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:02:00Z",
                        "finished_at": "2026-03-07T10:03:00Z",
                        "function_names": ["fn-3"],
                        "status": "succeeded",
                    },
                ],
            }
        )
        builder = ImageBuilderV3Client(
            cloud_client=cloud_client,
            build_service_path="/images/v3/applications",
        )

        result = await builder.build(
            ApplicationBuildRequest(
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
        )

        cloud_client.create_application_build.assert_called_once_with(
            "/images/v3/applications",
            json.dumps(
                {
                    "name": "app_fn",
                    "version": "v1",
                    "images": [
                        {
                            "key": "img-1",
                            "name": "image-a",
                            "context_tar_part_name": "img-1",
                            "context_sha256": "sha-a",
                            "function_names": ["fn-1", "fn-2"],
                        },
                        {
                            "key": "img-2",
                            "name": "image-b",
                            "context_tar_part_name": "img-2",
                            "context_sha256": "sha-b",
                            "function_names": ["fn-3"],
                        },
                    ],
                }
            ),
            [("img-1", b"context-a"), ("img-2", b"context-b")],
        )
        cloud_client.stream_build_logs_to_stderr_prefixed.assert_any_call(
            "/images/v3", "img-build-1", "image-a[img-1]", "magenta"
        )
        cloud_client.application_build_info_json.assert_called_once_with(
            "/images/v3/applications",
            "app-build-1",
        )
        self.assertEqual(result.id, "app-build-1")
        self.assertEqual(result.organization_id, "org-1")
        self.assertEqual(result.project_id, "proj-1")
        self.assertEqual(result.name, "app_fn")
        self.assertEqual(result.version, "v1")
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(len(result.image_builds), 2)
        self.assertEqual(result.image_builds[0].id, "img-build-1")
        self.assertEqual(result.image_builds[0].app_version_id, "app-version-1")
        self.assertEqual(result.image_builds[0].description, "Image A")
        self.assertEqual(result.image_builds[0].status, "succeeded")

    async def test_build_raises_application_image_build_error_for_failed_image(self):
        cloud_client = MagicMock()
        cloud_client.create_application_build.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "building",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "app_version_id": "app-version-1",
                        "key": "img-1",
                        "name": "image-a",
                        "status": "building",
                        "function_names": ["fn-1"],
                    }
                ],
            }
        )
        cloud_client.application_build_info_json.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "failed",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "app_version_id": "app-version-1",
                        "key": "img-1",
                        "name": "image-a",
                        "status": "failed",
                        "error_message": "docker build failed",
                        "function_names": ["fn-1"],
                    }
                ],
            }
        )
        builder = ImageBuilderV3Client(
            cloud_client=cloud_client,
            build_service_path="/images/v3/applications",
        )

        with self.assertRaisesRegex(Exception, "docker build failed"):
            await builder.build(
                ApplicationBuildRequest(
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
            )

    async def test_build_cancels_application_build_on_stream_cancellation(self):
        cloud_client = MagicMock()
        cloud_client.create_application_build.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "building",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "app_version_id": "app-version-1",
                        "key": "img-1",
                        "name": "image-a",
                        "status": "building",
                        "function_names": ["fn-1"],
                    }
                ],
            }
        )
        cloud_client.stream_build_logs_to_stderr_prefixed.side_effect = (
            asyncio.CancelledError
        )
        cloud_client.cancel_application_build.return_value = json.dumps(
            {
                "id": "app-build-1",
                "organization_id": "org-1",
                "project_id": "proj-1",
                "name": "app_fn",
                "version": "v1",
                "status": "canceled",
                "image_builds": [],
            }
        )
        builder = ImageBuilderV3Client(
            cloud_client=cloud_client,
            build_service_path="/images/v3/applications",
        )

        with self.assertRaises(asyncio.CancelledError):
            await builder.build(
                ApplicationBuildRequest(
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
            )

        cloud_client.cancel_application_build.assert_called_once_with(
            "/images/v3/applications",
            "app-build-1",
        )


if __name__ == "__main__":
    unittest.main()
