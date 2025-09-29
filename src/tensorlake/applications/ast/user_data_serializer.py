from ..function.user_data_serializer import function_output_serializer
from ..interface.function import Function
from ..registry import get_function
from .ast_node import ASTNode
from .function_call_node import RegularFunctionCallMetadata, RegularFunctionCallNode
from .reducer_call_node import ReducerFunctionCallMetadata, ReducerFunctionCallNode


def override_output_serializer_at_child_call_tree_root(
    function_output_serializer_name: str, function_output_ast: ASTNode
) -> None:
    """Propagates function output serializer (function_os) of parent function to the
    root node of call tree that the parent function returned.
    This is required i.e. for API functions that return a call to another function.
    The other function needs to return value in serialized using the parent API function output serializer.
    """
    if isinstance(function_output_ast, RegularFunctionCallNode):
        output_ast_root_metadata: RegularFunctionCallMetadata = (
            RegularFunctionCallMetadata.deserialize(
                function_output_ast.serialized_metadata
            )
        )
        output_ast_root_function: Function = get_function(
            function_output_ast.function_name
        )
        if function_output_serializer_name != (
            function_output_serializer(
                output_ast_root_function, output_ast_root_metadata.oso
            ).name
        ):
            output_ast_root_metadata.oso = function_output_serializer_name
            function_output_ast.serialized_metadata = (
                output_ast_root_metadata.serialize()
            )
    elif isinstance(function_output_ast, ReducerFunctionCallNode):
        output_ast_root_metadata: ReducerFunctionCallMetadata = (
            ReducerFunctionCallMetadata.deserialize(
                function_output_ast.serialized_metadata
            )
        )
        output_ast_root_function: Function = get_function(
            function_output_ast.reducer_function_name
        )
        if function_output_serializer_name != (
            function_output_serializer(
                output_ast_root_function, output_ast_root_metadata.oso
            ).name
        ):
            output_ast_root_metadata.oso = function_output_serializer_name
            function_output_ast.serialized_metadata = (
                output_ast_root_metadata.serialize()
            )
