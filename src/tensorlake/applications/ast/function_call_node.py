from typing import Any, Dict, List

from pydantic import BaseModel

from ..function.user_data_serializer import function_input_serializer
from ..interface.function import Function
from ..interface.futures import Collection, RegularFunctionCall
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer
from .ast import ast_from_user_object
from .ast_node import ASTNode, ASTNodeMetadata
from .collection import CollectionMetadata
from .value_node import ValueNode


class ArgumentMetadata(BaseModel):
    # None if the value is not coming from a particular child node.
    nid: str | None
    # Not None if this argument is coming from a Collection.
    # Once server/runtime understands collections they'll have their own AST node type.
    # And this field will be removed.
    col: CollectionMetadata | None


class RegularFunctionCallMetadata(ASTNodeMetadata):
    # Output serializer name override if any.
    oso: str | None
    args: List[ArgumentMetadata]
    kwargs: Dict[str, ArgumentMetadata]


class RegularFunctionCallNode(ASTNode):
    def __init__(self, node_id: str, function_name: str, start_delay: float | None):
        super().__init__(node_id)
        self._function_name: str = function_name
        self._start_delay: float | None = start_delay

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def start_delay(self) -> float | None:
        return self._start_delay

    @property
    def metadata(self) -> RegularFunctionCallMetadata:
        return self._metadata

    def to_regular_function_call(self) -> RegularFunctionCall:
        """Converts the AST node back to its original RegularFunctionCall.

        All children must be value nodes (they must already be resolved/finished).
        """
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
                if arg_metadata.col is not None:
                    collection_metadata: CollectionMetadata = arg_metadata.col
                    arg: List[Any] = []
                    for collection_item_node_id in collection_metadata.nids:
                        child_node: ValueNode = self.children[collection_item_node_id]
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

        process_arguments(self.metadata.args)
        process_arguments(self.metadata.kwargs)

        return RegularFunctionCall(
            function_name=self.function_name,
            args=args,
            kwargs=kwargs,
            start_delay=self.start_delay,
        )

    @classmethod
    def from_regular_function_call(
        cls, function_call: RegularFunctionCall
    ) -> "RegularFunctionCallNode":
        function: Function = get_function(function_call._function_name)
        inputs_serializer: UserDataSerializer = function_input_serializer(function)
        node: RegularFunctionCallNode = RegularFunctionCallNode(
            node_id=function_call.id,
            function_name=function_call.function_name,
            start_delay=function_call.start_delay,
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
                if isinstance(value, Collection):
                    collection_items_node_ids: List[str] = []
                    for item in value.items:
                        item_node: ASTNode = ast_from_user_object(
                            item, inputs_serializer
                        )
                        node.children[item_node.id] = item_node
                        item_node.parent = node
                        collection_items_node_ids.append(item_node.id)
                    arg_metadata = ArgumentMetadata(
                        nid=None,
                        col=CollectionMetadata(nids=collection_items_node_ids),
                    )
                else:
                    arg_node: ASTNode = ast_from_user_object(value, inputs_serializer)
                    node.add_child(arg_node)
                    arg_metadata = ArgumentMetadata(nid=arg_node.id, col=None)

                if isinstance(arguments, dict):
                    kwargs[key] = arg_metadata
                else:
                    args.append(arg_metadata)

        process_arguments(function_call.args)
        process_arguments(function_call.kwargs)

        node.metadata = RegularFunctionCallMetadata(
            nid=node.id,
            oso=None,  # Set by the node parent after this node is created.
            args=args,
            kwargs=kwargs,
        ).serialize()

        return node

    @classmethod
    def from_serialized(
        cls,
        function_name: str,
        metadata: bytes,
        children: List[ValueNode],
    ) -> "RegularFunctionCallNode":
        metadata: RegularFunctionCallMetadata = RegularFunctionCallMetadata.deserialize(
            metadata
        )
        # Start delay is not relevant when we run deserialized function call node.
        node: RegularFunctionCallNode = RegularFunctionCallNode(
            node_id=metadata.nid, function_name=function_name, start_delay=None
        )
        node.metadata = metadata
        for child in children:
            node.add_child(child)
        return node
