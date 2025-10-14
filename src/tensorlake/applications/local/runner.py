import contextvars
import traceback
from queue import SimpleQueue
from typing import Any, Dict, List

from ..ast import (
    ASTNode,
    ReducerFunctionCallMetadata,
    ReducerFunctionCallNode,
    RegularFunctionCallMetadata,
    RegularFunctionCallNode,
    ValueMetadata,
    ValueNode,
    ast_from_user_object,
    override_output_serializer_at_child_call_tree_root,
)
from ..function.application_call import (
    application_function_call_with_serialized_payload,
    deserialize_application_call_output,
    serialize_application_call_payload,
)
from ..function.function_call import (
    create_self_instance,
    set_self_arg,
)
from ..function.reducer_call import reducer_function_call
from ..function.type_hints import function_return_type_hint
from ..function.user_data_serializer import (
    function_input_serializer,
    function_output_serializer,
)
from ..interface.exceptions import RequestError, RequestFailureException
from ..interface.file import File
from ..interface.function import Function
from ..interface.futures import (
    FunctionCallFuture,
    Future,
    ReduceOperationFuture,
)
from ..interface.request import Request
from ..interface.request_context import RequestContext
from ..interface.retries import Retries
from ..registry import get_function
from ..request_context.contextvar import set_current_request_context
from ..request_context.request_context_base import RequestContextBase
from ..request_context.request_metrics_recorder import RequestMetricsRecorder
from ..runtime_hooks import (
    set_start_and_wait_function_calls_hook,
    set_start_function_calls_hook,
    set_wait_futures_hook,
)
from ..user_data_serializer import UserDataSerializer
from .blob_store import BLOB, BLOBStore
from .function_run import LocalFunctionRun, LocalFunctionRunResult
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

_LOCAL_REQUEST_ID = "local-request"


class LocalRunner:
    def __init__(self, app: Function, app_payload: Any):
        self._app: Function = app
        self._app_payload: Any = app_payload
        # Future ID -> BLOB if future succeeded.
        self._blob_store: BLOBStore = BLOBStore()
        # Futures currently known to the runner.
        # Future ID -> Future
        self._futures: Dict[str, FunctionCallFuture | ReduceOperationFuture] = {}
        # Function runs that are currently running or completed already.
        # Future ID -> LocalFunctionRun
        self._function_runs: Dict[str, LocalFunctionRun] = {}
        # Future ID -> exception
        # There's an entry if function run failed.
        self._function_run_exceptions: Dict[
            str, RequestFailureException | RequestError
        ] = {}
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        self._request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
            state=LocalRequestState(),
            progress=LocalRequestProgress(),
            metrics=RequestMetricsRecorder(),
        )
        # SimpleQueue[LocalFunctionRunResult]
        self._function_run_result_queue: SimpleQueue = SimpleQueue()

    def run(self) -> Request:
        try:
            input_serializer: UserDataSerializer = function_input_serializer(self._app)
            serialized_payload: bytes
            content_type: str
            serialized_payload, content_type = serialize_application_call_payload(
                input_serializer, self._app_payload
            )
            app_function_call: FunctionCallFuture = (
                application_function_call_with_serialized_payload(
                    application=self._app,
                    payload=serialized_payload,
                    payload_content_type=content_type,
                )
            )
            self._futures[app_function_call.id] = app_function_call

            set_start_function_calls_hook(self._runtime_hook_start_function_calls)
            set_start_and_wait_function_calls_hook(
                self._runtime_hook_start_and_wait_function_calls
            )
            set_wait_futures_hook(self._runtime_hook_wait_futures)

            self._run()

            if app_function_call.id in self._function_run_exceptions:
                return LocalRequest(
                    id=_LOCAL_REQUEST_ID,
                    output=None,
                    exception=self._function_run_exceptions[app_function_call.id],
                )

            app_output_blob: BLOB = self._blob_store.get(app_function_call.id)
            output: File | Any = deserialize_application_call_output(
                serialized_output=app_output_blob.data,
                serialized_output_content_type=app_output_blob.content_type,
                return_type_hints=function_return_type_hint(self._app),
                # No output serializer override for application functions.
                output_serializer=function_output_serializer(self._app, None),
            )

            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=output,
                exception=None,
            )
        except BaseException as e:
            # We only print exceptions in remote mode, do the same here.
            traceback.print_exception(e)
            return LocalRequest(
                id=_LOCAL_REQUEST_ID,
                output=None,
                exception=RequestFailureException("Request failed"),
            )

    def _runtime_hook_start_function_calls(
        self, function_calls: List[FunctionCallFuture | ReduceOperationFuture]
    ) -> None:
        pass

    def _runtime_hook_start_and_wait_function_calls(
        self, function_calls: List[FunctionCallFuture | ReduceOperationFuture]
    ) -> None:
        pass

    def _runtime_hook_wait_futures(self, futures: List[Future]) -> None:
        pass

    def _finished(self) -> bool:
        for future in self._futures.values():
            if future.id not in self._function_runs:
                return False

        return all(fr.finished for fr in self._function_runs.values())

    def _run(self) -> None:
        while not self._finished():
            for future in self._futures.values():
                if future.id in self._function_runs:
                    continue

                pass

    def _replace_node(self, old: ASTNode, new: ASTNode) -> None:
        if old is self._root_node:
            self._root_node = new
        else:
            old.parent.replace_child(old, new)

    def _run_regular_function_call(self, node: RegularFunctionCallNode) -> None:
        node_metadata: RegularFunctionCallMetadata = (
            RegularFunctionCallMetadata.deserialize(node.serialized_metadata)
        )
        function_call: FunctionCallFuture = node.to_regular_function_call()
        function: Function = get_function(function_call._function_name)
        function_os: UserDataSerializer = function_output_serializer(
            function, node_metadata.oso
        )
        output: Any = self._call(function_call, function)
        output_ast: ASTNode = ast_from_user_object(output, function_os)
        override_output_serializer_at_child_call_tree_root(
            function_output_serializer_name=function_os.name,
            function_output_ast=output_ast,
        )
        self._replace_node(node, output_ast)

    def _run_reducer_function_call(self, node: ReducerFunctionCallNode) -> None:
        node_metadata: ReducerFunctionCallMetadata = (
            ReducerFunctionCallMetadata.deserialize(node.serialized_metadata)
        )
        reducer_call: ReduceOperationFuture = node.to_reducer_function_call()
        reducer_function: Function = get_function(reducer_call.function_name)

        # inputs contains at least 2 items, this is guranteed by ReducerFunctionCall.
        inputs: List[Any] = reducer_call.inputs.items
        accumulator: Any = inputs[0]
        for input_value in inputs[1:]:
            function_call: FunctionCallFuture = reducer_function_call(
                reducer_function, accumulator, input_value
            )
            accumulator = self._call(function_call, reducer_function)

        reducer_function_os: UserDataSerializer = function_output_serializer(
            reducer_function, node_metadata.oso
        )
        output_ast: ASTNode = ast_from_user_object(
            accumulator,
            reducer_function_os,
        )
        override_output_serializer_at_child_call_tree_root(
            function_output_serializer_name=reducer_function_os.name,
            function_output_ast=output_ast,
        )
        self._replace_node(node, output_ast)

    def _call(self, function_call: FunctionCallFuture, function: Function) -> Any:
        self._set_function_call_instance_args(function_call, function)
        context: contextvars.Context = contextvars.Context()

        # Application retries are used if function retries are not set.
        function_retries: Retries = (
            self._app._application_config.retries
            if function._function_config.retries is None
            else function._function_config.retries
        )
        runs_left: int = 1 + function_retries.max_retries
        while True:
            try:
                return context.run(self._call_with_context, function_call, function)
            except Exception:
                runs_left -= 1
                if runs_left == 0:
                    raise

    def _call_with_context(
        self, function_call: FunctionCallFuture, function: Function
    ) -> Any:
        # This function is executed in contextvars.Context of the Tensorlake Function call.
        set_current_request_context(self._request_context)
        return function._original_function(
            *function_call._args, **function_call._kwargs
        )

    def _set_function_call_instance_args(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        if function._function_config.class_name is None:
            return

        if function._function_config.class_name not in self._class_instances:
            self._class_instances[function._function_config.class_name] = (
                create_self_instance(function._function_config.class_name)
            )

        set_self_arg(
            function_call, self._class_instances[function._function_config.class_name]
        )


def _find_non_value_node_with_value_only_children(ast: ASTNode) -> ASTNode | None:
    if isinstance(ast, ValueNode):
        return None

    all_children_are_values: bool = True
    for child in ast.children.values():
        child: ASTNode
        if not isinstance(child, ValueNode):
            all_children_are_values = False

        child_result: ASTNode | None = _find_non_value_node_with_value_only_children(
            child
        )
        if child_result is not None:
            return child_result

    return ast if all_children_are_values else None
