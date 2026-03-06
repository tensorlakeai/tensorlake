import asyncio
import json
import sys

from pydantic import BaseModel

from tensorlake.builder import ApplicationBuildRequest
from tensorlake.cloud_client import CloudClient


class ApplicationBuildImageResult(BaseModel):
    id: str
    app_version_id: str | None = None
    key: str | None = None
    name: str | None = None
    description: str | None = None
    status: str
    error_message: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    finished_at: str | None = None
    function_names: list[str] | None = None


class ApplicationBuildResult(BaseModel):
    id: str
    organization_id: str | None = None
    project_id: str | None = None
    name: str
    version: str
    status: str | None = None
    image_builds: list[ApplicationBuildImageResult]


class ImageBuilderV3Client:
    def __init__(
        self,
        cloud_client: CloudClient,
        build_service_path: str = "/images/v3/applications",
    ):
        self._cloud_client = cloud_client
        self._build_service_path = build_service_path

    async def build(self, request: ApplicationBuildRequest) -> ApplicationBuildResult:
        print(
            f"Python ImageBuilderV3Client.build called for {request.name}@{request.version}",
            file=sys.stderr,
        )
        request_json = json.dumps(
            {
                "name": request.name,
                "version": request.version,
                "images": [
                    {
                        "key": image.key,
                        "name": image.name,
                        "context_tar_part_name": image.key,
                        "context_sha256": image.context_sha256,
                        "function_names": image.function_names,
                    }
                    for image in request.images
                ],
            }
        )
        image_contexts = [
            (image.key, image.context_tar_gz) for image in request.images
        ]
        response_json = await asyncio.to_thread(
            self._cloud_client.create_application_build,
            self._build_service_path,
            request_json,
            image_contexts,
        )
        return ApplicationBuildResult.model_validate_json(response_json)
