from typing import Any

import pydantic

from ..function.user_data_serializer import serialize_value
from ..interface.request import Request
from ..metadata import ValueMetadata
from ..user_data_serializer import (
    PickleUserDataSerializer,
    UserDataSerializer,
    serializer_by_name,
)
from .api_client import APIClient
from .app_manifest_cache import get_app_manifest, has_app_manifest, set_app_manifest
from .manifests.application import ApplicationManifest
from .request import RemoteRequest


def _convert_pydantic_to_dict(obj: Any) -> Any:
    """Recursively converts Pydantic models to dicts.

    This is used for pickle serialization to avoid class reference issues
    when sending Pydantic instances across process boundaries.
    """
    if isinstance(obj, pydantic.BaseModel):
        return obj.model_dump()
    elif isinstance(obj, dict):
        return {k: _convert_pydantic_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_pydantic_to_dict(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(_convert_pydantic_to_dict(item) for item in obj)
    return obj


class RemoteRunner:
    def __init__(
        self,
        application_name: str,
        payload: Any,
        api_client: APIClient,
    ):
        self._application_name: str = application_name
        self._payload: Any = payload
        self._client: APIClient = api_client

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

        # For pickle serializer, convert Pydantic models to dicts to avoid
        # class reference issues across process boundaries. The FE will
        # reconstruct Pydantic models using the serializer and type hints.
        payload_to_serialize = self._payload
        if isinstance(input_serializer, PickleUserDataSerializer):
            payload_to_serialize = _convert_pydantic_to_dict(self._payload)

        serialized_payload: bytes
        metadata: ValueMetadata
        serialized_payload, metadata = serialize_value(
            value=payload_to_serialize, serializer=input_serializer, value_id="fake_id"
        )
        if metadata.content_type is None:
            metadata.content_type = input_serializer.content_type

        request_id: str = self._client.run_request(
            application_name=self._application_name,
            input=serialized_payload,
            input_content_type=metadata.content_type,
        )
        return RemoteRequest(
            application_name=self._application_name,
            application_manifest=app_manifest,
            request_id=request_id,
            client=self._client,
        )
