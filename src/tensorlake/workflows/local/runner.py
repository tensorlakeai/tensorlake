from typing import Any, Dict, List

from ..ast import (
    ASTNode,
    ReducerFunctionCallNode,
    RegularFunctionCallNode,
    ValueNode,
    ast_from_user_object,
)
from ..function.function_call import (
    create_self_instance,
    set_request_context_args,
    set_self_arg,
)
from ..function.reducer_call import reducer_function_call
from ..function.user_data_serializer import (
    function_input_serializer,
    function_output_serializer,
)
from ..interface.function import Function
from ..interface.function_call import (
    FunctionCall,
    RegularFunctionCall,
)
from ..interface.reduce import ReducerFunctionCall
from ..interface.request import Request
from ..interface.request_context import RequestContext
from ..registry import get_function
from ..request_context_base import RequestContextBase
from ..request_metrics_recorder import RequestMetricsRecorder
from .request import LocalRequest
from .request_progress import LocalRequestProgress
from .request_state import LocalRequestState

_LOCAL_REQUEST_ID = "local-request"


# We're using AST in local mode even though it's not the most convenient way for local mode.
# Is is to get as similar experience as possible with remote mode where AST is used.
class LocalRunner:
    def __init__(self):
        # AST we're running.
        self._root_node: ASTNode = None
        # Class name => instance.
        self._class_instances: Dict[str, Any] = {}
        # Function name -> serialized current accumulator value.
        self._reducer_accumulators: Dict[str, bytes] = {}
        self._request_context: RequestContext = RequestContextBase(
            request_id=_LOCAL_REQUEST_ID,
            state=LocalRequestState(),
            progress=LocalRequestProgress(),
            metrics=RequestMetricsRecorder(),
        )

    def run(self, function_call: FunctionCall) -> Request:
        try:
            function: Function = get_function(function_call.function_name)
            self._root_node = ast_from_user_object(
                function_call, function_input_serializer(function)
            )
            return self._run()
        except BaseException as e:
            return LocalRequest(id=_LOCAL_REQUEST_ID, output=None, exception=e)

    def _run(self) -> Request:
        while not isinstance(self._root_node, ValueNode):
            next_node: ASTNode | None = _find_non_value_node_with_value_only_children(
                self._root_node
            )
            if next_node is None:
                raise ValueError(
                    "All nodes are values, in this case there should be only one root value node in the graph left"
                )

            if isinstance(next_node, RegularFunctionCallNode):
                self._run_regular_function_call(next_node)
            elif isinstance(next_node, ReducerFunctionCallNode):
                self._run_reducer_function_call(next_node)
            else:
                raise ValueError(f"Unexpected node type: {type(next_node)}")

        if not isinstance(self._root_node, ValueNode):
            raise RuntimeError(
                "AST root is not a value node after executing the request, this is an internal bug or a malformed function call"
            )

        self._root_node: ValueNode
        return LocalRequest(
            id=_LOCAL_REQUEST_ID,
            output=self._root_node.to_value(),
            exception=None,
        )

    def _replace_node(self, old: ASTNode, new: ASTNode) -> None:
        if old is self._root_node:
            self._root_node = new
        else:
            old.parent.replace_child(old, new)

    def _run_regular_function_call(self, node: RegularFunctionCallNode) -> None:
        function_call: RegularFunctionCall = node.to_regular_function_call()
        function: Function = get_function(function_call.function_name)
        output: Any = self._call(function_call, function)
        output_ast: ASTNode = ast_from_user_object(
            output, function_output_serializer(function)
        )
        self._replace_node(node, output_ast)

    def _run_reducer_function_call(self, node: ReducerFunctionCallNode) -> None:
        reducer_call: ReducerFunctionCall = node.to_reducer_function_call()
        reducer_function: Function = get_function(reducer_call.function_name)

        # inputs contains at least 1 item, this is guranteed by ReducerFunctionCall.
        inputs: List[Any] = reducer_call.inputs.items
        accumulator: Any = inputs[0]
        for input_value in inputs[1:]:
            function_call: RegularFunctionCall = reducer_function_call(
                reducer_function, accumulator, input_value
            )
            accumulator = self._call(function_call, reducer_function)

        output_ast: ASTNode = ast_from_user_object(
            accumulator,
            function_output_serializer(reducer_function),
        )
        self._replace_node(node, output_ast)

    def _call(self, function_call: RegularFunctionCall, function: Function) -> Any:
        set_request_context_args(function_call, self._request_context)
        self._set_function_call_instance_args(function_call, function)

        runs_left: int = 1 + function.function_config.retries.max_retries
        while True:
            try:
                return function.original_function(
                    *function_call.args, **function_call.kwargs
                )
            except Exception:
                runs_left -= 1
                if runs_left == 0:
                    raise

    def _set_function_call_instance_args(
        self, function_call: FunctionCall, function: Function
    ) -> None:
        if function.function_config.class_name is None:
            return

        if function.function_config.class_name not in self._class_instances:
            self._class_instances[function.function_config.class_name] = (
                create_self_instance(function.function_config.class_name)
            )

        set_self_arg(
            function_call, self._class_instances[function.function_config.class_name]
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
