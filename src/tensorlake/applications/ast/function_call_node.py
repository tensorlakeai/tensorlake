import pickle
from typing import Any, Dict, List

from pydantic import BaseModel

from ..function.user_data_serializer import function_input_serializer
from ..interface.function import Function
from ..interface.function_call import RegularFunctionCall
from ..interface.future import FutureList
from ..interface.request_context import RequestContext
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer
from .ast import ast_from_user_object
from .ast_node import ASTNode
from .future_list_metadata import FutureListMetadata
from .value_node import ValueNode


class ArgumentMetadata(BaseModel):
    # None if the value is not coming from a particular child node.
    nid: str | None
    # Not None if this argument is coming from a FutureList.
    flist: FutureListMetadata | None


class RegularFunctionCallMetadata(BaseModel):
    # Output serializer name override if any.
    oso: str | None
    args: List[ArgumentMetadata]
    kwargs: Dict[str, ArgumentMetadata]

    def serialize(self) -> bytes:
        return pickle.dumps(self)

    @classmethod
    def deserialize(cls, data: bytes) -> "RegularFunctionCallMetadata":
        return pickle.loads(data)


class RegularFunctionCallNode(ASTNode):
    def __init__(self, function_name: str):
        super().__init__()
        self._function_name = function_name

    @property
    def function_name(self) -> str:
        return self._function_name

    def to_regular_function_call(self) -> RegularFunctionCall:
        """Converts the AST node back to its original RegularFunctionCall.

        All children must be value nodes (they must already be resolved/finished).
        """
        metadata: RegularFunctionCallMetadata = RegularFunctionCallMetadata.deserialize(
            self.serialized_metadata
        )
        args: List[Any] = []
        kwargs: Dict[str, Any] = {}

        def process_arguments(
            arguments: List[ArgumentMetadata] | Dict[str, ArgumentMetadata],
        ):
            for key, arg_metadata in (
                arguments.items()
                if isinstance(arguments, dict)
                else enumerate(arguments)
            ):
                arg_metadata: ArgumentMetadata
                arg: Any
                if arg_metadata.flist is not None:
                    future_list_metadata: FutureListMetadata = arg_metadata.flist
                    arg: List[Any] = []
                    for future_list_node_id in future_list_metadata.nids:
                        child_node: ValueNode = self.children[future_list_node_id]
                        arg.append(child_node.to_value())
                elif arg_metadata.nid is not None:
                    child_node: ValueNode = self.children[arg_metadata.nid]
                    arg = child_node.to_value()
                else:
                    raise ValueError(f"Bad argument metadata {arg_metadata}")

                if isinstance(arguments, dict):
                    kwargs[key] = arg
                else:
                    args.append(arg)

        process_arguments(metadata.args)
        process_arguments(metadata.kwargs)

        return RegularFunctionCall(
            function_name=self.function_name, args=args, kwargs=kwargs
        )

    @classmethod
    def from_regular_function_call(
        cls, function_call: RegularFunctionCall
    ) -> "RegularFunctionCallNode":
        function: Function = get_function(function_call.function_name)
        inputs_serializer: UserDataSerializer = function_input_serializer(function)
        node: RegularFunctionCallNode = RegularFunctionCallNode(
            function_call.function_name
        )
        args: List[ArgumentMetadata] = []
        # Arg name -> Arg metadata.
        kwargs: Dict[str, ArgumentMetadata] = {}

        def process_arguments(arguments: List[Any] | Dict[str, Any]):
            for key, value in (
                arguments.items()
                if isinstance(arguments, dict)
                else enumerate(arguments)
            ):
                arg_metadata: ArgumentMetadata
                if isinstance(value, FutureList):
                    future_list_node_ids: List[str] = []
                    for item in value.items:
                        item_node: ASTNode = ast_from_user_object(
                            item, inputs_serializer
                        )
                        node.children[item_node.id] = item_node
                        item_node.parent = node
                        future_list_node_ids.append(item_node.id)
                    arg_metadata = ArgumentMetadata(
                        nid=None,
                        flist=FutureListMetadata(nids=future_list_node_ids),
                    )
                else:
                    arg_node: ASTNode = ast_from_user_object(value, inputs_serializer)
                    node.add_child(arg_node)
                    arg_metadata = ArgumentMetadata(nid=arg_node.id, flist=None)

                if isinstance(arguments, dict):
                    kwargs[key] = arg_metadata
                else:
                    args.append(arg_metadata)

        process_arguments(function_call.args)
        process_arguments(function_call.kwargs)

        node.serialized_metadata = RegularFunctionCallMetadata(
            oso=None,  # Set by the node parent after this node is created.
            args=args,
            kwargs=kwargs,
        ).serialize()

        return node

    @classmethod
    def from_serialized(
        cls,
        node_id: str,
        function_name: str,
        metadata: bytes,
        children: List[ValueNode],
    ) -> "RegularFunctionCallNode":
        node: RegularFunctionCallNode = RegularFunctionCallNode(function_name)
        node._id = node_id
        node.serialized_metadata = metadata
        for child in children:
            node.add_child(child)
        return node
