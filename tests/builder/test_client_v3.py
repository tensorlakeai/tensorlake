import asyncio
import json
import unittest
from unittest.mock import MagicMock, patch

from tensorlake.builder import ApplicationBuildImageRequest, ApplicationBuildRequest
from tensorlake.builder.client_v3 import ImageBuilderV3Client
from tensorlake.builder.log_events import BuildLogEvent

SHA_A = "a" * 64
SHA_B = "b" * 64


class TestImageBuilderV3Client(unittest.IsolatedAsyncioTestCase):
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
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:01:00Z",
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
                "created_at": "2026-03-07T10:00:00Z",
                "updated_at": "2026-03-07T10:02:00Z",
                "finished_at": "2026-03-07T10:03:00Z",
                "image_builds": [
                    {
                        "id": "img-build-1",
                        "key": "img-1",
                        "name": "image-a",
                        "context_sha256": SHA_A,
                        "status": "failed",
                        "error_message": "docker build failed",
                        "image_uri": "registry.example.com/app/image-a:latest",
                        "image_digest": None,
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:02:00Z",
                        "finished_at": "2026-03-07T10:03:00Z",
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
                            context_sha256=SHA_A,
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
                        "created_at": "2026-03-07T10:00:00Z",
                        "updated_at": "2026-03-07T10:01:00Z",
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
                "created_at": "2026-03-07T10:00:00Z",
                "updated_at": "2026-03-07T10:02:00Z",
                "finished_at": "2026-03-07T10:03:00Z",
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
                            context_sha256=SHA_A,
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

    async def test_stream_build_logs_falls_back_to_json_events(self):
        cloud_client = MagicMock()
        cloud_client.stream_build_logs_to_stderr_prefixed.side_effect = RuntimeError(
            "sse stream failed"
        )
        cloud_client.stream_build_logs_json.return_value = [
            json.dumps(
                {
                    "image_build_id": "img-build-1",
                    "timestamp": "2026-03-07T10:01:00Z",
                    "stream": "stdout",
                    "message": "step 1",
                    "sequence_number": 1,
                    "build_status": "building",
                }
            )
        ]
        builder = ImageBuilderV3Client(
            cloud_client=cloud_client,
            build_service_path="/images/v3/applications",
        )
        reporter = builder._build_reporters(
            type(
                "Result",
                (),
                {
                    "image_builds": [
                        type(
                            "ImageBuild",
                            (),
                            {
                                "id": "img-build-1",
                                "key": "img-1",
                                "name": "image-a",
                                "status": "pending",
                            },
                        )()
                    ],
                    "name": "app_fn",
                },
            )()
        )["img-build-1"]

        with patch.object(reporter, "print_log_event") as print_log_event:
            await builder._stream_build_logs(reporter)

        cloud_client.stream_build_logs_to_stderr_prefixed.assert_called_once_with(
            "/images/v3",
            "img-build-1",
            "app_fn/image-a",
            "magenta",
        )
        cloud_client.stream_build_logs_json.assert_called_once_with(
            "/images/v3",
            "img-build-1",
        )
        print_log_event.assert_called_once()
        self.assertEqual(
            print_log_event.call_args.args[0],
            BuildLogEvent(
                image_build_id="img-build-1",
                timestamp="2026-03-07T10:01:00Z",
                stream="stdout",
                message="step 1",
                sequence_number=1,
                build_status="building",
            ),
        )

    def test_reporter_print_log_event_handles_pending_and_enqueued(self):
        builder = ImageBuilderV3Client(
            cloud_client=MagicMock(),
            build_service_path="/images/v3/applications",
        )
        reporter = builder._build_reporters(
            type(
                "Result",
                (),
                {
                    "image_builds": [
                        type(
                            "ImageBuild",
                            (),
                            {
                                "id": "img-build-1",
                                "key": "img-1",
                                "name": "image-a",
                                "status": "pending",
                            },
                        )()
                    ],
                    "name": "app_fn",
                },
            )()
        )["img-build-1"]

        with patch("tensorlake.builder.client_v3._print_message") as print_message:
            reporter.print_log_event(
                BuildLogEvent(
                    image_build_id="img-build-1",
                    timestamp="2026-03-07T10:01:00Z",
                    stream="info",
                    message="queued",
                    sequence_number=1,
                    build_status="pending",
                )
            )
            reporter.print_log_event(
                BuildLogEvent(
                    image_build_id="img-build-1",
                    timestamp="2026-03-07T10:02:00Z",
                    stream="info",
                    message="still queued",
                    sequence_number=2,
                    build_status="enqueued",
                )
            )

        waiting_calls = [
            call
            for call in print_message.call_args_list
            if call.args and call.args[0] == "Build waiting in queue..."
        ]
        self.assertEqual(len(waiting_calls), 2)

    async def test_build_rejects_duplicate_function_names_across_images(self):
        builder = ImageBuilderV3Client(
            cloud_client=MagicMock(),
            build_service_path="/images/v3/applications",
        )

        with self.assertRaisesRegex(ValueError, "function_names must be unique"):
            await builder.build(
                ApplicationBuildRequest(
                    name="app_fn",
                    version="v1",
                    images=[
                        ApplicationBuildImageRequest(
                            key="img-1",
                            name="image-a",
                            context_sha256="a" * 64,
                            function_names=["fn-shared"],
                            context_tar_gz=b"context-a",
                        ),
                        ApplicationBuildImageRequest(
                            key="img-2",
                            name="image-b",
                            context_sha256="b" * 64,
                            function_names=["fn-shared"],
                            context_tar_gz=b"context-b",
                        ),
                    ],
                )
            )


if __name__ == "__main__":
    unittest.main()
