from typing import Any

from ..function.application_call import serialize_application_call_payload
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .api_client import APIClient
from .app_manifest_cache import get_app_manifest, has_app_manifest, set_app_manifest
from .manifests.application import ApplicationManifest
from .request import RemoteRequest


class RemoteRunner:
    def __init__(
        self,
        application_name: str,
        payload: Any,
    ):
        self._application_name: str = application_name
        self._payload: Any = payload
        self._client: APIClient = APIClient()

    def run(self) -> Request:
        if not has_app_manifest(self._application_name):
            app_manifest: ApplicationManifest = self._client.application(
                self._application_name
            )
            set_app_manifest(self._application_name, app_manifest)

        app_manifest: ApplicationManifest = get_app_manifest(self._application_name)
        input_serializer: UserDataSerializer = serializer_by_name(
            app_manifest.entrypoint.input_serializer
        )

        serialized_payload: bytes
        content_type: str
        serialized_payload, content_type = serialize_application_call_payload(
            input_serializer, self._payload
        )

        request_id: str = self._client.call(
            application_name=self._application_name,
            payload=serialized_payload,
            payload_content_type=content_type,
        )
        return RemoteRequest(
            application_name=self._application_name,
            application_manifest=app_manifest,
            request_id=request_id,
            client=self._client,
        )
