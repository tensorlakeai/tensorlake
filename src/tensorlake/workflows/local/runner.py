from typing import Any, Dict, List

from ..function_call import (
    REDUCER_ACCUMULATOR_PARAMETER_NAME,
    REDUCER_IS_LAST_VALUE_PARAMETER_NAME,
    FunctionOutputs,
    create_default_accumulator_value,
    create_self_instance,
    extract_function_outputs,
    fill_with_request_context_placeholders,
    function_kwarg_type_hint,
    set_request_context_args,
    set_self_arg,
)
from ..interface.exceptions import RequestException
from ..interface.file import File
from ..interface.function import Function
from ..interface.function_call import FunctionCall
from ..interface.request import Request
from ..interface.request_context import RequestContext
from ..registry import get_function
from ..request_context_base import RequestContextBase
from ..user_data_serializer import UserDataSerializer, serializer_by_name
from .function_call import LocalFunctionCall
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

_LOCAL_REQUEST_ID = "local-request"


# We're storing everything using local function calls and outputs because
# they store user data in serialized form. This provides more consistent UX
# between remote and local modes.
class LocalRunner:
    def __init__(self):
        # Pending calls.
        self._reducer_calls: List[LocalFunctionCall] = []
        self._function_calls: List[LocalFunctionCall] = []
        # Function name -> serialized output values or File.
        self._function_output_values: Dict[str, List[bytes | File]] = {}
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        # Function name -> serialized current accumulator value.
        self._reducer_accumulators: Dict[str, bytes] = {}
        # Key -> Serialized value.
        self._request_state: Dict[str, bytes] = {}

    def run(self, function_call: FunctionCall) -> Request:
        # The first function call passed to us might contain real request context objects.
        fill_with_request_context_placeholders(function_call)
        self._enqueue_first_function_call(function_call)
        return self._run()

    def _enqueue_first_function_call(self, function_call: FunctionCall) -> None:
        function: Function = get_function(function_call.function_name)
        user_serializer: UserDataSerializer = serializer_by_name(
            function.function_config.input_serializer
        )
        local_function_call: LocalFunctionCall = LocalFunctionCall.from_function_call(
            function_call, user_serializer
        )
        if function.reducer_config is not None:
            self._reducer_calls.append(local_function_call)
        else:
            self._function_calls.append(local_function_call)

    def _run(self) -> Request:
        while self._function_calls or self._reducer_calls:
            # A reducer can only be finished when there are no pending function calls that might produce the reducer call.
            local_function_call: LocalFunctionCall = (
                self._function_calls.pop(0)
                if self._function_calls
                else self._reducer_calls.pop(0)
            )
            function: Function = get_function(local_function_call.function_name)
            user_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.input_serializer
            )
            self._do_function_call(
                local_function_call.to_function_call(function, user_serializer),
                function,
            )

        return LocalRequest(
            id=_LOCAL_REQUEST_ID, function_output_values=self._function_output_values
        )

    def _do_function_call(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        self._set_function_call_request_context(function_call, function)
        self._set_function_call_instance_args(function_call)
        self._set_function_call_reducer_args(function_call, function)

        output: Any = function.original_function(
            *function_call.args, **function_call.kwargs
        )
        function_outputs: FunctionOutputs = extract_function_outputs(output)
        self._process_value_outputs(function_call, function, function_outputs.values)
        self._process_file_outputs(function_call, function_outputs.files)
        self._process_function_call_outputs(function_outputs.function_calls)

        self._process_function_call_reducer_outputs(
            function_call, function, function_outputs.values
        )

    def _set_function_call_instance_args(self, function_call: FunctionCall) -> None:
        if function_call.class_name is None:
            return

        if function_call.class_name not in self._class_instances:
            self._class_instances[function_call.class_name] = create_self_instance(
                function_call
            )

        set_self_arg(function_call, self._class_instances[function_call.class_name])

    def _set_function_call_request_context(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
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
        set_request_context_args(function_call, request_context)

    def _set_function_call_reducer_args(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        if function.reducer_config is None:
            return

        if function_call.function_name in self._reducer_accumulators:
            user_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.input_serializer
            )
            deserialized_accumulator: Any = user_serializer.deserialize(
                self._reducer_accumulators[function_call.function_name],
                function_kwarg_type_hint(function, REDUCER_ACCUMULATOR_PARAMETER_NAME),
            )
            function_call.kwargs[REDUCER_ACCUMULATOR_PARAMETER_NAME] = (
                deserialized_accumulator
            )
        else:
            function_call.kwargs[REDUCER_ACCUMULATOR_PARAMETER_NAME] = (
                create_default_accumulator_value(function)
            )

        pending_reducer_calls: int = 0
        for pending_reducer_call in self._reducer_calls:
            pending_reducer_call: LocalFunctionCall
            if pending_reducer_call.function_name == function_call.function_name:
                pending_reducer_calls += 1

        function_call.kwargs[REDUCER_IS_LAST_VALUE_PARAMETER_NAME] = (
            pending_reducer_calls == 0
        )

    def _process_function_call_outputs(
        self,
        new_function_calls: List[FunctionCall],
    ) -> None:
        for new_function_call in new_function_calls:
            fill_with_request_context_placeholders(new_function_call)
            called_function: Function = get_function(new_function_call.function_name)
            user_serializer: UserDataSerializer = serializer_by_name(
                called_function.function_config.input_serializer
            )
            local_function_call: LocalFunctionCall = (
                LocalFunctionCall.from_function_call(new_function_call, user_serializer)
            )

            if called_function.reducer_config is not None:
                self._reducer_calls.append(local_function_call)
            else:
                self._function_calls.append(local_function_call)

    def _process_value_outputs(
        self, function_call: FunctionCall, function: Function, values: List[Any]
    ) -> None:
        if function_call.function_name not in self._function_output_values:
            self._function_output_values[function_call.function_name] = []

        user_serializer: UserDataSerializer = serializer_by_name(
            function.function_config.output_serializer
        )
        for value in values:
            self._function_output_values[function_call.function_name].append(
                user_serializer.serialize(value)
            )

    def _process_file_outputs(
        self, function_call: FunctionCall, files: List[File]
    ) -> None:
        if function_call.function_name not in self._function_output_values:
            self._function_output_values[function_call.function_name] = []

        for file in files:
            self._function_output_values[function_call.function_name].append(file)

    def _process_function_call_reducer_outputs(
        self,
        function_call: FunctionCall,
        function: Function,
        output_values: List[Any],
    ) -> None:
        if function.reducer_config is None:
            return

        if function_call.kwargs[REDUCER_IS_LAST_VALUE_PARAMETER_NAME]:
            del self._reducer_accumulators[function_call.function_name]
            return
            # Last function call outputs are interpreted as usual function call outputs.

        if len(output_values) != 1:
            raise RequestException(
                f"Reducer function `{function_call.function_name}` returned {len(output_values)} values, "
                "please return only one (new accumulator value)."
            )

        user_serializer: UserDataSerializer = serializer_by_name(
            function.function_config.output_serializer
        )
        # This is the new accumulator value.
        self._reducer_accumulators[function_call.function_name] = (
            user_serializer.serialize(output_values[0])
        )
