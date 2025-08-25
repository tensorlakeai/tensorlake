from typing import Any, Dict, List

from ..interface.function import Function
from ..interface.function_call import FunctionCall
from ..interface.package import Package
from ..interface.request import Request
from ..interface.request_context import RequestContext, RequestContextPlaceholder
from ..registry import get_class, get_function
from ..request_context_base import RequestContextBase
from ..serialization.function_call import SerializableFunctionCall
from ..serialization.function_output import FunctionOutput, FunctionOutputType
from ..serialization.user_data_serializer import UserDataSerializer, serializer_by_name
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

_LOCAL_REQUEST_ID = "local-request"


class LocalRunner:
    def __init__(self, package: Package, function_call: FunctionCall):
        self._api_name: str = ""
        self._package: Package = package
        # Pending reducer calls as serialized SerializableFunctionCall.
        self._reducer_calls: List[bytes] = []
        # Pending function calls as serialized SerializableFunctionCall.
        self._function_calls: List[bytes] = []
        # Function name -> list of serialized FunctionOutput.
        self._outputs: Dict[str, List[List[bytes]]] = {}
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        # Function name -> serialized current accumulator value.
        self._reducer_accumulators: Dict[str, bytes] = {}
        # Key -> Serialized value.
        self._request_state: Dict[str, bytes] = {}

        self._enqueue_first_function_call(function_call)

    def _enqueue_first_function_call(self, function_call: FunctionCall) -> None:
        self._put_function_call_request_context_placeholders(function_call)
        function: Function = get_function(function_call.function_name)
        user_serializer: UserDataSerializer = serializer_by_name(
            function.function_config.input_serializer
        )
        serialized_function_call: SerializableFunctionCall = (
            SerializableFunctionCall.from_function_call(
                function_call, user_serializer
            ).serialize()
        )
        if function.reducer_config is not None:
            self._reducer_calls.append(serialized_function_call)
        else:
            self._function_calls.append(serialized_function_call)

        if function.api_config is None:
            self._api_name = "The first called function is not an API function"
        else:
            self._api_name = function.function_config.function_name

    def run(self) -> Request:
        while self._function_calls or self._reducer_calls:
            # A reducer can only be finished when there are no pending function calls that might produce the reducer call.
            serialized_function_call: bytes = (
                self._function_calls.pop(0)
                if self._function_calls
                else self._reducer_calls.pop(0)
            )
            serializable_function_call: SerializableFunctionCall = (
                SerializableFunctionCall.deserialize(serialized_function_call)
            )
            function: Function = get_function(serializable_function_call.function_name)
            user_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.input_serializer
            )
            function_call: FunctionCall = serializable_function_call.to_function_call(
                user_serializer
            )

            self._do_function_call(function_call)

        return LocalRequest(id=_LOCAL_REQUEST_ID, outputs=self._outputs)

    def _do_function_call(self, function_call: FunctionCall) -> None:
        function: Function = get_function(function_call.function_name)
        self._set_function_call_request_context(function_call, function)
        self._set_function_call_instance_args(function_call, function)
        self._set_function_call_reducer_args(function_call, function)

        function_outputs: List[Any] | Any = function.original_function(
            *function_call.args, **function_call.kwargs
        )
        if not isinstance(function_outputs, list):
            function_outputs = [function_outputs]
        function_outputs: List[Any]

        self._process_function_call_outputs(function_call, function, function_outputs)
        self._process_function_call_reducer_outputs(
            function_call, function, function_outputs
        )

    def _set_function_call_instance_args(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        if function_call.class_name is None:
            return

        if function_call.class_name not in self._class_instances:
            # TODO: Raise RequestError with a clear description if the class is not found and class_name is not None.
            # Right now an Exception is raised from get_class without details.
            cls: Any = get_class(function_call.class_name)
            instance: Any = cls()  # Calling our empty constructor here
            instance.__tensorlake_original_init__()
            self._class_instances[function_call.class_name] = instance

        function_call.args.insert(0, self._class_instances[function_call.class_name])

    def _set_function_call_request_context(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
            api_name=self._api_name,
            api_version=self._package.version,
            state=LocalRequestState(
                input_serializer=serializer_by_name(
                    function.function_config.input_serializer
                ),
                output_serializer=serializer_by_name(
                    function.function_config.output_serializer
                ),
                initial_state=self._request_state,
            ),
            progress=LocalRequestProgress(),
        )

        for ix, arg in enumerate(function_call.args):
            if isinstance(arg, RequestContextPlaceholder):
                function_call.args[ix] = request_context
        for key, value in function_call.kwargs.items():
            if isinstance(value, RequestContextPlaceholder):
                function_call.kwargs[key] = request_context

    def _set_function_call_reducer_args(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        if function.reducer_config is None:
            return

        # Otherwise, the default accumulator parameter value will be used.
        if function_call.function_name in self._reducer_accumulators:
            user_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.input_serializer
            )
            deserialized_accumulator: Any = user_serializer.deserialize(
                self._reducer_accumulators[function_call.function_name]
            )
            function_call.kwargs["accumulator"] = deserialized_accumulator

        pending_reducer_calls: int = 0
        for pending_reducer_call in self._reducer_calls:
            serializable_function_call: SerializableFunctionCall = (
                SerializableFunctionCall.deserialize(pending_reducer_call)
            )
            if serializable_function_call.function_name == function_call.function_name:
                pending_reducer_calls += 1

        function_call.kwargs["is_last_value"] = pending_reducer_calls == 0

    def _process_function_call_outputs(
        self,
        function_call: FunctionCall,
        function: Function,
        function_outputs: List[Any],
    ) -> None:
        if function_call.function_name not in self._outputs:
            self._outputs[function_call.function_name] = []

        serialized_outputs: List[bytes] = []
        for output in function_outputs:
            if isinstance(output, FunctionCall):
                serialized_outputs.append(
                    self._process_function_call_output(output).serialize()
                )
            else:
                serialized_outputs.append(
                    self._process_value_output(function, output).serialize()
                )

        self._outputs[function_call.function_name].append(serialized_outputs)

    def _process_function_call_output(
        self, new_function_call: FunctionCall
    ) -> FunctionOutput:
        self._put_function_call_request_context_placeholders(new_function_call)
        called_function: Function = get_function(new_function_call.function_name)
        user_serializer: UserDataSerializer = serializer_by_name(
            called_function.function_config.input_serializer
        )
        serializable_function_call: SerializableFunctionCall = (
            SerializableFunctionCall.from_function_call(
                new_function_call, user_serializer
            )
        )
        function_output: FunctionOutput = FunctionOutput(
            type=FunctionOutputType.FUNCTION_CALL,
            function_call=serializable_function_call,
            value=None,
        )

        if called_function.reducer_config is not None:
            self._reducer_calls.append(serializable_function_call.serialize())
        else:
            self._function_calls.append(serializable_function_call.serialize())

        return function_output

    def _process_value_output(
        self, finished_function: Function, value: Any
    ) -> FunctionOutput:
        user_serializer: UserDataSerializer = serializer_by_name(
            finished_function.function_config.output_serializer
        )
        serialized_value: bytes = user_serializer.serialize(value)
        return FunctionOutput(
            type=FunctionOutputType.VALUE,
            function_call=None,
            value=serialized_value,
        )

    def _process_function_call_reducer_outputs(
        self,
        function_call: FunctionCall,
        function: Function,
        function_outputs: List[Any],
    ) -> None:
        if function.reducer_config is None:
            return

        for output in function_outputs:
            if not isinstance(output, FunctionCall):
                user_serializer: UserDataSerializer = serializer_by_name(
                    function.function_config.output_serializer
                )
                # This is the new accumulator value.
                self._reducer_accumulators[function_call.function_name] = (
                    user_serializer.serialize(output)
                )
                # There should be only one non-FunctionCall output from a reducer function.
                break

    def _put_function_call_request_context_placeholders(
        self, function_call: FunctionCall
    ) -> None:
        for ix, arg in enumerate(function_call.args):
            if isinstance(arg, RequestContext):
                function_call.args[ix] = RequestContextPlaceholder()
        for key, value in function_call.kwargs.items():
            if isinstance(value, RequestContext):
                function_call.kwargs[key] = RequestContextPlaceholder()
