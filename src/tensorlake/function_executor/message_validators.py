from .proto.function_executor_pb2 import (
    Allocation,
    FunctionRef,
    InitializeRequest,
    SerializedObject,
    SerializedObjectEncoding,
)
from .proto.message_validator import MessageValidator


def validate_new_allocation(allocation: Allocation):
    """Validates the incoming allocation before creating it.

    Raises ValueError if the allocation is invalid.
    """
    # Validate required fields
    (
        MessageValidator(allocation)
        .required_field("request_id")
        .required_field("function_call_id")
        .required_field("allocation_id")
        .required_field("inputs")
        .not_set_field("result")
    )

    # Validate allocation inputs
    (
        MessageValidator(allocation.inputs)
        .optional_serialized_objects_inside_blob("args")
        .optional_blobs("arg_blobs")
        .required_blob("request_error_blob")
    )
    if len(allocation.inputs.args) != len(allocation.inputs.arg_blobs):
        raise ValueError(
            "Mismatched function arguments and functions argument blobs lengths, "
            f"{len(allocation.inputs.args)} != {len(allocation.inputs.arg_blobs)}"
        )


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
