from .proto.function_executor_pb2 import (
    FunctionRef,
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
            self._message_validator.required_field(
                "function"
            ).required_serialized_object("application_code")
        )
        self._validate_function_ref()
        self._validate_application_code()

    def _validate_function_ref(self):
        function_ref: FunctionRef = self._request.function
        (
            MessageValidator(function_ref)
            .required_field("namespace")
            .required_field("application_name")
            .required_field("function_name")
            .required_field("application_version")
        )

    def _validate_application_code(self):
        application_code: SerializedObject = self._request.application_code
        if (
            application_code.manifest.encoding
            != SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_ZIP
        ):
            raise ValueError(
                f"Invalid application code encoding: {SerializedObjectEncoding.Name(application_code.manifest.encoding)}. Expected: BINARY_ZIP"
            )
