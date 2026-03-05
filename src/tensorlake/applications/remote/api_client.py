import json
import os

from pydantic import BaseModel

from tensorlake.applications.interface.exceptions import (
    InternalError,
)
from tensorlake.applications.interface.exceptions import (
    RequestError as RequestErrorException,
)
from tensorlake.applications.interface.exceptions import (
    RequestFailed,
    RequestNotFinished,
)
from tensorlake.applications.remote.manifests.application import ApplicationManifest
from tensorlake.cloud_client import CloudClient

_API_NAMESPACE_FROM_ENV: str | None = os.getenv("INDEXIFY_NAMESPACE", "default")
_API_URL_FROM_ENV: str = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
_API_KEY_ENVIRONMENT_VARIABLE_NAME = "TENSORLAKE_API_KEY"
_API_KEY_FROM_ENV: str | None = os.getenv(_API_KEY_ENVIRONMENT_VARIABLE_NAME)


class Application(BaseModel):
    name: str
    description: str
    tags: dict[str, str]
    version: str
    tombstoned: bool = False
    created_at: int | None = None


class RequestError(BaseModel):
    function_name: str
    message: str


class RequestMetadata(BaseModel):
    id: str
    # dict when failure outcome
    # str when success outcome
    # None when not finished
    outcome: dict | str | None = None
    application_version: str
    created_at: int
    request_error: RequestError | None = None


class RequestInput(BaseModel):
    data: bytes
    content_type: str
    name: str


class RequestOutput(BaseModel):
    serialized_value: bytes
    content_type: str


class APIClient:
    def __init__(
        self,
        api_url: str = _API_URL_FROM_ENV,
        api_key: str | None = _API_KEY_FROM_ENV,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = _API_NAMESPACE_FROM_ENV,
    ):
        self._cloud_client = CloudClient(
            api_url=api_url,
            api_key=api_key,
            organization_id=organization_id,
            project_id=project_id,
            namespace=namespace,
        )

    def __enter__(self) -> "APIClient":
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        self._cloud_client.close()

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        self._cloud_client.upsert_application(
            manifest_json=manifest_json,
            code_zip=code_zip,
            upgrade_running_requests=upgrade_running_requests,
        )

    def delete_application(
        self,
        application_name: str,
    ) -> None:
        self._cloud_client.delete_application(application_name=application_name)

    def applications(self) -> list[Application]:
        response_json: str = self._cloud_client.applications_json()
        application_jsons: list[dict] = json.loads(response_json)["applications"]
        return [Application.model_validate(app) for app in application_jsons]

    def application(self, application_name: str) -> ApplicationManifest:
        response_json: str = self._cloud_client.application_manifest_json(
            application_name=application_name
        )
        return ApplicationManifest.model_validate_json(response_json)

    def run_request(
        self,
        application_name: str,
        inputs: list[RequestInput],
    ) -> str:
        rust_inputs: list[tuple[str, bytes, str]] = [
            (part.name, part.data, part.content_type) for part in inputs
        ]
        return self._cloud_client.run_request(
            application_name=application_name,
            inputs=rust_inputs,
        )

    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
    ):
        self._cloud_client.wait_on_request_completion(
            application_name=application_name,
            request_id=request_id,
        )

    def request_output(
        self,
        application_name: str,
        request_id: str,
    ) -> RequestOutput:
        request_metadata_json: str = self._cloud_client.request_metadata_json(
            application_name=application_name,
            request_id=request_id,
        )
        request_metadata: RequestMetadata = RequestMetadata.model_validate_json(
            request_metadata_json
        )

        if request_metadata.outcome is None:
            raise RequestNotFinished()

        if isinstance(request_metadata.outcome, dict):
            if request_metadata.request_error is None:
                raise RequestFailed(request_metadata.outcome["failure"])
            else:
                raise RequestErrorException(request_metadata.request_error.message)

        serialized_value_raw, content_type = self._cloud_client.request_output_bytes(
            application_name=application_name,
            request_id=request_id,
        )
        if isinstance(serialized_value_raw, (bytes, bytearray, memoryview)):
            serialized_value: bytes = bytes(serialized_value_raw)
        elif isinstance(serialized_value_raw, list):
            serialized_value = bytes(serialized_value_raw)
        else:
            raise InternalError(
                "Unexpected request output payload type from Rust Cloud SDK: "
                f"{type(serialized_value_raw).__name__}"
            )
        return RequestOutput(
            serialized_value=serialized_value,
            content_type=content_type,
        )
