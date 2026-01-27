import base64
from typing import Any

from tensorlake.applications.interface.exceptions import InternalError, SDKUsageError

from ..function.application_call import (
    ApplicationArgument,
    serialize_application_function_call_arguments,
)
from ..interface.request import Request
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .api_client import APIClient, RequestInput
from .app_manifest_cache import get_app_manifest, has_app_manifest, set_app_manifest
from .manifests.application import (
    ApplicationManifest,
    EntryPointInputManifest,
    deserialize_input_manifests,
)
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
        input_manifests: list[EntryPointInputManifest] = deserialize_input_manifests(
            base64.decodebytes(app_manifest.entrypoint.inputs_base64.encode("utf-8"))
        )

        serialized_args, serialized_kwargs = (
            serialize_application_function_call_arguments(
                input_serializer=input_serializer,
                args=self._make_application_args(input_manifests),
                kwargs=self._make_application_kwargs(input_manifests),
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

    def _make_application_args(
        self, input_manifests: list[EntryPointInputManifest]
    ) -> list[ApplicationArgument]:
        args: list[ApplicationArgument] = []
        for arg_index, arg_value in enumerate(self._args):
            if arg_index >= len(input_manifests):
                # Allow users to pass unknown args and ignore them instead of failing.
                # This gives them more flexibility i.e. when they change their code but
                # didn't change request payload yet.
                continue

            arg_manifest: EntryPointInputManifest = input_manifests[arg_index]
            args.append(
                ApplicationArgument(value=arg_value, type_hint=arg_manifest.type_hint)
            )

        return args

    def _make_application_kwargs(
        self, input_manifests: list[EntryPointInputManifest]
    ) -> dict[str, ApplicationArgument]:
        kwargs: dict[str, ApplicationArgument] = {}
        for kwarg_name, kwarg_value in self._kwargs.items():
            arg_manifest: EntryPointInputManifest | None = None
            for input_manifest in input_manifests:
                if input_manifest.arg_name == kwarg_name:
                    arg_manifest = input_manifest
                    break

            if arg_manifest is None:
                # Allow users to pass unknown args and ignore them instead of failing.
                # This gives them more flexibility i.e. when they changes their code but
                # didn't change request payload yet.
                continue

            kwargs[kwarg_name] = ApplicationArgument(
                value=kwarg_value, type_hint=arg_manifest.type_hint
            )

        return kwargs
