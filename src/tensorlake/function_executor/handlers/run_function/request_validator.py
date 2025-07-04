from ...proto.function_executor_pb2 import RunTaskRequest
from ...proto.message_validator import MessageValidator


class RequestValidator:
    def __init__(self, request: RunTaskRequest):
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
            .required_field("graph_invocation_id")
            .required_field("task_id")
            .required_serialized_object("function_input")
            .optional_serialized_object("function_init_value")
        )
        # TODO: make allocation_id required once Executor is updated to pass it.
