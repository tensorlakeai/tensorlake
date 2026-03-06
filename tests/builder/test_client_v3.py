import json
import unittest
from unittest.mock import MagicMock

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.client_v3 import ImageBuilderV3Client


class TestImageBuilderV3Client(unittest.IsolatedAsyncioTestCase):
    async def test_build_translates_request_with_image_contexts_and_parses_response(self):
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
                    )
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
                        }
                    ],
                }
            ),
            [("img-1", b"context-a"), ("img-2", b"context-b")],
        )
        self.assertEqual(result.id, "app-build-1")
        self.assertEqual(result.organization_id, "org-1")
        self.assertEqual(result.project_id, "proj-1")
        self.assertEqual(result.name, "app_fn")
        self.assertEqual(result.version, "v1")
        self.assertEqual(result.status, "building")
        self.assertEqual(len(result.image_builds), 1)
        self.assertEqual(result.image_builds[0].id, "img-build-1")
        self.assertEqual(result.image_builds[0].app_version_id, "app-version-1")
        self.assertEqual(result.image_builds[0].description, "Image A")
        self.assertEqual(result.image_builds[0].status, "pending")


if __name__ == "__main__":
    unittest.main()
