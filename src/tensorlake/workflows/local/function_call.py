from typing import Any, Dict, List

from ..function_call import function_arg_type_hint, function_kwarg_type_hint
from ..interface.function import Function
from ..interface.function_call import FunctionCall
from ..interface.request_context import RequestContextPlaceholder
from ..user_data_serializer import UserDataSerializer


class LocalFunctionCall:
    def __init__(
        self,
        class_name: str | None,
        function_name: str,
        args: List[bytes],
        kwargs: Dict[str, bytes],
    ):
        # None if the called function is not a method.
        self._class_name = class_name
        self._function_name = function_name
        self._args = args
        self._kwargs = kwargs
        self._request_context_arg_indexes: List[int] = []
        self._request_context_kwarg_keys: List[str] = []

    @property
    def class_name(self) -> str | None:
        return self._class_name

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def args(self) -> List[bytes]:
        return self._args

    @property
    def kwargs(self) -> Dict[str, bytes]:
        return self._kwargs

    def to_function_call(
        self, function: Function, user_serializer: UserDataSerializer
    ) -> FunctionCall:
        return FunctionCall(
            class_name=self.class_name,
            function_name=self.function_name,
            args=[
                self._deserialize_arg(ix, arg, function, user_serializer)
                for ix, arg in enumerate(self.args)
            ],
            kwargs={
                k: self._deserialize_kwarg(k, v, function, user_serializer)
                for k, v in self.kwargs.items()
            },
        )

    @classmethod
    def from_function_call(
        cls, function_call: FunctionCall, user_serializer: UserDataSerializer
    ) -> "LocalFunctionCall":
        instance: LocalFunctionCall = LocalFunctionCall(
            class_name=function_call.class_name,
            function_name=function_call.function_name,
            args=[],
            kwargs={},
        )
        for ix, arg in enumerate(function_call.args):
            instance._args.append(instance._serialize_arg(ix, arg, user_serializer))
        for k, v in function_call.kwargs.items():
            instance._kwargs[k] = instance._serialize_kwarg(k, v, user_serializer)

        return instance

    def _serialize_arg(
        self, arg_ix: int, arg: Any, user_serializer: UserDataSerializer
    ) -> bytes:
        if isinstance(arg, RequestContextPlaceholder):
            # Request context is not json serializable, store None and record where it was.
            self._request_context_arg_indexes.append(arg_ix)
            return user_serializer.serialize(None)
        else:
            return user_serializer.serialize(arg)

    def _serialize_kwarg(
        self, key: str, value: Any, user_serializer: UserDataSerializer
    ) -> bytes:
        if isinstance(value, RequestContextPlaceholder):
            # Request context is not json serializable, store None and record where it was.
            self._request_context_kwarg_keys.append(key)
            return user_serializer.serialize(None)
        else:
            return user_serializer.serialize(value)

    def _deserialize_arg(
        self,
        arg_ix: int,
        arg: bytes,
        function: Function,
        user_serializer: UserDataSerializer,
    ) -> Any:
        if arg_ix in self._request_context_arg_indexes:
            return RequestContextPlaceholder()
        else:
            return user_serializer.deserialize(
                arg, function_arg_type_hint(function, arg_ix)
            )

    def _deserialize_kwarg(
        self,
        key: str,
        value: bytes,
        function: Function,
        user_serializer: UserDataSerializer,
    ) -> Any:
        if key in self._request_context_kwarg_keys:
            return RequestContextPlaceholder()
        else:
            return user_serializer.deserialize(
                value, function_kwarg_type_hint(function, key)
            )
