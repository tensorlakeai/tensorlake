from typing import Any

from ..function.application_call import serialize_application_function_call_arguments
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .api_client import APIClient, RequestInput
from .app_manifest_cache import get_app_manifest, has_app_manifest, set_app_manifest
from .manifests.application import ApplicationManifest
from .request import RemoteRequest


class RemoteRunner:
    def __init__(
        self,
        application_name: str,
        args: list[Any],
        kwargs: dict[str, Any],
        api_client: APIClient,
    ):
        self._application_name: str = application_name
        self._args: list[Any] = args
        self._kwargs: dict[str, Any] = kwargs
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
        serialized_args, serialized_kwargs = (
            serialize_application_function_call_arguments(
                input_serializer=input_serializer,
                args=self._args,
                kwargs=self._kwargs,
            )
        )

        inputs: list[RequestInput] = []
        for idx, serialized_arg in enumerate(serialized_args):
            inputs.append(
                RequestInput(
                    name=str(idx),
                    data=serialized_arg.data,
                    content_type=serialized_arg.content_type,
                )
            )
        # kwarg key can't start with a digit so no confusion with args.
        for key, serialized_kwarg in serialized_kwargs.items():
            inputs.append(
                RequestInput(
                    name=key,
                    data=serialized_kwarg.data,
                    content_type=serialized_kwarg.content_type,
                )
            )

        request_id: str = self._client.run_request(
            application_name=self._application_name,
            inputs=inputs,
        )

        return RemoteRequest(
            application_name=self._application_name,
            application_manifest=app_manifest,
            request_id=request_id,
            client=self._client,
        )
