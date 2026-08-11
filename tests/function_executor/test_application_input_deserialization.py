from tensorlake.applications import HttpBody, application, function
from tensorlake.function_executor.allocation_runner.sdk_algorithms import (
    deserialize_application_function_call_args,
)
from tensorlake.function_executor.allocation_runner.value import SerializedValue


@application()
@function()
def http_body_input(body: HttpBody) -> None:
    pass


def test_http_body_preserves_absent_content_type() -> None:
    args, kwargs = deserialize_application_function_call_args(
        function=http_body_input,
        payload=SerializedValue(
            metadata=None,
            data=b"",
            content_type=None,
        ),
        function_instance_arg=None,
    )

    assert kwargs == {}
    assert len(args) == 1
    assert isinstance(args[0], HttpBody)
    assert args[0].content == b""
    assert args[0].content_type is None


def test_parameterized_message_http_content_type_is_forwarded() -> None:
    request = (
        b"POST /invoke HTTP/1.1\r\n"
        b"Host: compatibility.tensorlake\r\n"
        b"Content-Type: application/octet-stream\r\n"
        b"\r\n"
        b"payload"
    )

    args, kwargs = deserialize_application_function_call_args(
        function=http_body_input,
        payload=SerializedValue(
            metadata=None,
            data=request,
            content_type="message/http; msgtype=request",
        ),
        function_instance_arg=None,
    )

    assert kwargs == {}
    assert len(args) == 1
    assert isinstance(args[0], HttpBody)
    assert args[0].content == b"payload"
    assert args[0].content_type == "application/octet-stream"
