from ..proto.function_executor_pb2 import RequestStateResponse
from ..proto.message_validator import MessageValidator


class ResponseValidator(MessageValidator):
    def __init__(self, response: RequestStateResponse):
        self._response: RequestStateResponse = response

    def check(self):
        """Validates the request.

        Raises: ValueError: If the response is invalid.
        """
        (
            MessageValidator(self._response)
            .required_field("state_request_id")
            .required_field("success")
        )

        if self._response.HasField("set"):
            pass
        elif self._response.HasField("get"):
            (
                MessageValidator(self._response.get)
                .required_field("key")
                .optional_serialized_object("value")
            )
        else:
            raise ValueError(f"Unknown response type: {self._response}")
