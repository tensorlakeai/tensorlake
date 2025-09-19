from typing import Any

from ..application import get_user_defined_or_default_application
from ..function.api_call import api_function_call_with_serialized_payload
from ..function.user_data_serializer import function_input_serializer
from ..local.runner import LocalRunner
from ..registry import get_function
from ..remote.runner import RemoteRunner
from .application import Application
from .function import Function
from .function_call import FunctionCall
from .request import Request


def call_local_api(api: Function | str, payload: Any) -> Request:
    """Calls the API locally and returns the request."""
    # TODO: validate the graph.
    # TODO: validate that the supplied function is an API function.

    if isinstance(api, str):
        api: Function = get_function(api)
    # Serialize payload first to make local UX and remote UX as similar as possible.
    serialized_payload: bytes = function_input_serializer(api).serialize(payload)
    return LocalRunner().run(
        api_function_call_with_serialized_payload(api, serialized_payload)
    )


def call_remote_api(api: Function | str, payload: Any) -> Request:
    """Calls the API remotely (i.e. on Tensorlake Cloud) and returns the request."""
    # TODO: validate the graph.
    # TODO: validate that the supplied function is an API function.
    application: Application = get_user_defined_or_default_application()
    if isinstance(api, str):
        api: Function = get_function(api)
    return RemoteRunner(application, api, payload).run()


def call_api(api: Function | str, payload: Any, remote: bool) -> Request:
    """Call the API remotely or locally depending on the `remote` parameter value.

    This is a convenience wrapper around the `call_remote_api` and `call_local_api`.
    """
    if remote:
        return call_remote_api(api, payload)
    else:
        return call_local_api(api, payload)


def call_local_function(function_call: FunctionCall) -> Request:
    """Runs the function call locally and returns the request.

    Primarily used for local debugging of individual functions.
    """
    return LocalRunner().run(function_call)
