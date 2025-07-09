from .proto.function_executor_pb2 import (
    InitializeRequest,
    SerializedObject,
    SerializedObjectEncoding,
)
from .proto.message_validator import MessageValidator


class InitializeRequestValidator:
    def __init__(self, request: InitializeRequest):
        self._request = request
        self._message_validator = MessageValidator(request)

    def check(self):
        """Validates the request.

        Raises: ValueError: If the request is invalid.
        """
        (
            self._message_validator.required_field("namespace")
            .required_field("graph_name")
            .required_field("graph_version")
            .required_field("function_name")
            .required_serialized_object("graph")
        )
        graph: SerializedObject = self._request.graph
        if (
            graph.encoding
            != SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP
        ):
            raise ValueError(
                f"Invalid graph encoding: {SerializedObjectEncoding.Name(graph.encoding)}. Expected: BINARY_ZIP"
            )
