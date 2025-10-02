from typing import Any

from ..function.application_call import (
    application_function_call_with_serialized_payload,
    serialize_application_call_payload,
)
from ..function.user_data_serializer import function_input_serializer
from ..local.runner import LocalRunner
from ..registry import get_function
from ..remote.runner import RemoteRunner
from ..user_data_serializer import UserDataSerializer
from .function import Function
from .request import Request


def run_local_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function locally with the given payload and returns the request."""
    # TODO: validate the graph.
    # TODO: validate that the supplied function is an API function.

    if isinstance(application, str):
        application: Function = get_function(application)

    input_serializer: UserDataSerializer = function_input_serializer(application)
    # Serialize payload first to make local UX and remote UX as similar as possible.
    serialized_payload: bytes
    content_type: str
    serialized_payload, content_type = serialize_application_call_payload(
        input_serializer, payload
    )
    return LocalRunner(application=application).run(
        application_function_call_with_serialized_payload(
            application=application,
            payload=serialized_payload,
            payload_content_type=content_type,
        )
    )


def run_remote_application(application: Function | str, payload: Any) -> Request:
    """Runs the application function remotely (i.e. on Tensorlake Cloud) with the given payload and returns the request."""
    # TODO: validate the graph.
    # TODO: validate that the supplied function is an API function.
    app_name: str = (
        application.function_config.function_name
        if isinstance(application, Function)
        else application
    )

    # We can't get Function object here because the user's client call might not load the function definitions.
    return RemoteRunner(application_name=app_name, payload=payload).run()


def run_application(application: Function | str, payload: Any, remote: bool) -> Request:
    """Runs the application remotely or locally depending on the `remote` parameter value.

    This is a convenience wrapper around the `run_remote_application` and `run_local_application`.
    """
    if remote:
        return run_remote_application(application, payload)
    else:
        return run_local_application(application, payload)
