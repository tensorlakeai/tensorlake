from __future__ import annotations

import argparse
import asyncio
import base64
import contextvars
import hashlib
import importlib
import inspect
import json
import logging
import os
import sys
import tempfile
import threading
import time
import weakref
import zipfile
from dataclasses import dataclass, field
from email.parser import BytesParser
from email.policy import default as email_policy
from typing import Any

from tensorlake._cloud_sdk import FunctionAgentCore
from tensorlake.applications import (
    RETURN_WHEN,
    FunctionError,
    Future,
    InternalError,
    RequestContext,
    RequestError,
    SDKUsageError,
    TimeoutError,
)
from tensorlake.applications.algorithms import dfs_bottom_up_unique_only
from tensorlake.applications.function.application_call import (
    SerializedApplicationArgument,
    deserialize_application_function_call_arguments,
)
from tensorlake.applications.function.function_call import (
    create_self_instance,
    set_self_arg,
)
from tensorlake.applications.function.type_hints import (
    function_signature,
    return_type_hint,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value_with_metadata,
    function_input_serializer,
    function_output_serializer,
    serialize_value,
)
from tensorlake.applications.interface.futures import (
    FunctionCallFuture,
    MapFuture,
    ReduceOperationFuture,
    _InitialMissing,
    _unwrap_future,
)
from tensorlake.applications.interface.request_context import (
    FunctionProgress,
    Headers,
    RequestMetrics,
    RequestState,
)
from tensorlake.applications.metadata import (
    SPLITTER_INPUT_MODE,
    FunctionCallArgumentMetadata,
    FunctionCallMetadata,
    ValueMetadata,
    deserialize_metadata,
    serialize_metadata,
)
from tensorlake.applications.multiprocessing import setup_multiprocessing
from tensorlake.applications.registry import get_function, get_functions, has_function
from tensorlake.applications.remote.code.zip import (
    CODE_ZIP_MANIFEST_FILE_NAME,
    CodeZIPManifest,
)
from tensorlake.applications.request_context.contextvar import (
    set_current_request_context,
)
from tensorlake.applications.request_context.request_state import (
    REQUEST_STATE_USER_DATA_SERIALIZER,
)
from tensorlake.applications.runtime_hooks import (
    set_await_future_hook,
    set_coroutine_to_future_hook,
    set_register_coroutine_hook,
    set_run_future_hook,
    set_wait_futures_hook,
)

LOGGER = logging.getLogger("tensorlake.function_agent.runner")
MAX_REQUEST_STATE_KEY_BYTES = 1024
MAX_REQUEST_STATE_VALUE_BYTES = 1024 * 1024
_CURRENT_ATTEMPT: contextvars.ContextVar["Attempt"] = contextvars.ContextVar(
    "tensorlake_function_agent_attempt"
)
_RUNTIME_HOOKS_LOCK = threading.Lock()
_RUNTIME_HOOKS_INSTALLED = False


def _b64decode(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"), validate=True)


def _b64encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


_RESERVED_ENVIRONMENT_TARGETS = {
    "PYTHONFAULTHANDLER",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "PYTHONHOME",
    "PYTHONPATH",
    "NODE_OPTIONS",
    "NODE_PATH",
}


def _apply_resolved_environment(assignment: dict[str, Any]) -> None:
    raw_environment = assignment.pop("resolved_environment", [])
    if not isinstance(raw_environment, list):
        raise ValueError("Resolved environment must be a list")
    resolved: list[tuple[str, str]] = []
    targets: set[str] = set()
    for item in raw_environment:
        if not isinstance(item, dict) or set(item) != {"target", "value"}:
            raise ValueError("Resolved environment entry is invalid")
        target = item["target"]
        value = item["value"]
        if (
            not isinstance(target, str)
            or not target
            or len(target) > 256
            or not (target[0].isalpha() or target[0] == "_")
            or not all(
                character.isascii() and (character.isalnum() or character == "_")
                for character in target
            )
            or target in _RESERVED_ENVIRONMENT_TARGETS
            or target.startswith("TENSORLAKE_")
            or target in targets
            or not isinstance(value, str)
            or "\x00" in value
        ):
            raise ValueError("Resolved environment entry is invalid")
        targets.add(target)
        resolved.append((target, value))
    for target, value in resolved:
        os.environ[target] = value
    for item in raw_environment:
        item["value"] = ""
    raw_environment.clear()


class ProtocolWriter:
    def __init__(self, core: FunctionAgentCore, loop: asyncio.AbstractEventLoop):
        self._core = core
        self._loop = loop
        self._lock = threading.Lock()

    def write(self, message: dict[str, Any]) -> None:
        encoded = json.dumps(message, separators=(",", ":"), ensure_ascii=False)

        async def submit_output() -> None:
            await self._core.submit_output(encoded)

        with self._lock:
            asyncio.run_coroutine_threadsafe(submit_output(), self._loop).result()


class RuntimeRequestState(RequestState):
    def __init__(self, attempt: "Attempt"):
        self._attempt = attempt

    def set(self, key: str, value: Any) -> None:
        self._validate_key(key)
        serialized = REQUEST_STATE_USER_DATA_SERIALIZER.serialize(value, type_hint=None)
        if len(serialized) > MAX_REQUEST_STATE_VALUE_BYTES:
            raise SDKUsageError(
                "Serialized request-state value contains "
                f"{len(serialized)} bytes; maximum is {MAX_REQUEST_STATE_VALUE_BYTES}"
            )
        result = self._attempt.request_state_operation(
            operation="set",
            key=key,
            value_base64=_b64encode(serialized),
        )
        if result.get("result") != "set":
            raise InternalError("Function Service returned an invalid state set result")

    def get(self, key: str, default: Any | None = None) -> Any | None:
        self._validate_key(key)
        result = self._attempt.request_state_operation(operation="get", key=key)
        if result.get("result") != "get":
            raise InternalError("Function Service returned an invalid state get result")
        encoded = result.get("value_base64")
        if encoded is None:
            return default
        serialized = _b64decode(encoded)
        return REQUEST_STATE_USER_DATA_SERIALIZER.deserialize(
            serialized, type_hint=type(default)
        )

    @staticmethod
    def _validate_key(key: str) -> None:
        if not isinstance(key, str):
            raise SDKUsageError(f"State key must be a string, got: {key}")
        key_bytes = len(key.encode("utf-8"))
        if key_bytes > MAX_REQUEST_STATE_KEY_BYTES:
            raise SDKUsageError(
                f"Request-state key contains {key_bytes} bytes; "
                f"maximum is {MAX_REQUEST_STATE_KEY_BYTES}"
            )


class RuntimeProgress(FunctionProgress):
    def __init__(self, attempt: "Attempt", protocol: ProtocolWriter):
        self._attempt = attempt
        self._protocol = protocol

    def update(
        self,
        current: int | float,
        total: int | float,
        message: str | None = None,
        attributes: dict[str, str] | None = None,
    ) -> None:
        if not isinstance(current, (int, float)) or not isinstance(total, (int, float)):
            raise SDKUsageError("progress current and total must be numbers")
        if attributes is not None and (
            not isinstance(attributes, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in attributes.items()
            )
        ):
            raise SDKUsageError("progress attributes must be string key/value pairs")
        if self._attempt.cancelled:
            return
        self._protocol.write(
            {
                "type": "progress",
                "attempt_id": self._attempt.attempt_id,
                "message": message if message is not None else f"{current}/{total}",
            }
        )


class RuntimeMetrics(RequestMetrics):
    def timer(self, name: str, value: int | float) -> None:
        if not isinstance(name, str) or not isinstance(value, (int, float)):
            raise SDKUsageError(
                "timer name must be a string and value must be a number"
            )

    def counter(self, name: str, value: int = 1) -> None:
        if not isinstance(name, str) or not isinstance(value, int):
            raise SDKUsageError(
                "counter name must be a string and value must be an int"
            )


class RuntimeRequestContext(RequestContext):
    def __init__(
        self,
        request_id: str,
        headers: list[dict[str, str]],
        state: RuntimeRequestState,
        progress: RuntimeProgress,
    ):
        self._request_id = request_id
        self._headers = Headers((header["name"], header["value"]) for header in headers)
        self._state = state
        self._progress = progress
        self._metrics = RuntimeMetrics()

    @property
    def request_id(self) -> str:
        return self._request_id

    @property
    def headers(self) -> Headers:
        return Headers(self._headers.multi_items())

    @property
    def state(self) -> RequestState:
        return self._state

    @property
    def progress(self) -> FunctionProgress:
        return self._progress

    @property
    def metrics(self) -> RequestMetrics:
        return self._metrics


@dataclass(frozen=True)
class CallReference:
    function_call_id: str


@dataclass(frozen=True)
class SpecialSettings:
    is_map_splitter: bool = False
    is_reduce_splitter: bool = False
    splitter_function_name: str | None = None
    splitter_input_mode: SPLITTER_INPUT_MODE | None = None
    is_map_concat: bool = False


@dataclass(frozen=True)
class CallSpec:
    function_call_id: str
    function_name: str
    args: list[Any | CallReference]
    kwargs: dict[str, Any | CallReference]
    is_tail_call: bool
    special_settings: SpecialSettings | None = None


@dataclass
class FutureInfo:
    future: Future
    watched: bool = False
    finished: bool = False
    completion_order: int = -1
    completion_callbacks: list[Any] = field(default_factory=list)


class RunnerCancelled(BaseException):
    pass


class Attempt:
    def __init__(
        self,
        runtime: "PythonFunctionRunner",
        assignment: dict[str, Any],
        function: Any,
        function_instance: Any | None,
    ):
        self.runtime = runtime
        self.assignment = assignment
        self.attempt_id = assignment["attempt_id"]
        self.function = function
        self.function_instance = function_instance
        self.cancelled = False
        self._condition = threading.Condition()
        self._runtime_hook_lock = threading.Lock()
        self._future_durable_ids: dict[str, str] = {}
        self._future_infos: dict[str, FutureInfo] = {}
        self._previous_future_durable_id = assignment["function_run_id"]
        self._completion_counter = 0
        self._coroutine_to_future: weakref.WeakKeyDictionary[Any, Future] = (
            weakref.WeakKeyDictionary()
        )
        self._output_serializer_name: str | None = None
        self._has_output_type_hint = False
        self._output_type_hint: Any = None
        self._special_settings: SpecialSettings | None = None

        self.request_context = RuntimeRequestContext(
            request_id=assignment["request_id"],
            headers=assignment.get("request_headers", []),
            state=RuntimeRequestState(self),
            progress=RuntimeProgress(self, runtime.protocol),
        )
        self._next_request_state_sequence = 1
        self._request_state_results: dict[str, dict[str, Any] | None] = {}

    def run(self) -> None:
        try:
            args, kwargs, self._special_settings = self._prepare_call()
            _CURRENT_ATTEMPT.set(self)
            set_current_request_context(self.request_context)
            output = self._call_user_function(args, kwargs)
            self._check_cancelled()
            output = _unwrap_future(output)
            if isinstance(output, Future):
                self._finish_tail_call(output)
            else:
                self._finish_value(output)
        except RunnerCancelled:
            LOGGER.info("attempt %s stopped after cancellation", self.attempt_id)
        except BaseException as error:
            if not self.cancelled:
                self._failure(error)
        finally:
            self.runtime.remove_attempt(self.attempt_id)

    def cancel(self) -> None:
        with self._condition:
            self.cancelled = True
            callbacks = [
                callback
                for info in self._future_infos.values()
                for callback in info.completion_callbacks
            ]
            for info in self._future_infos.values():
                info.completion_callbacks = []
            self._condition.notify_all()
        for callback in callbacks:
            callback()

    def deliver_result(self, message: dict[str, Any]) -> None:
        function_call_id = message["function_call_id"]
        with self._condition:
            info = self._future_infos.get(function_call_id)
            if info is None or info.finished or self.cancelled:
                return
            outcome = message["outcome"]
            if outcome == "success":
                metadata = deserialize_metadata(
                    _b64decode(message.get("metadata_base64", ""))
                )
                if not isinstance(metadata, ValueMetadata):
                    info.future._set_exception(
                        InternalError("Function output is missing ValueMetadata")
                    )
                else:
                    try:
                        info.future._set_result(
                            deserialize_value_with_metadata(
                                _b64decode(message["output_base64"]), metadata
                            )
                        )
                    except BaseException as error:
                        info.future._set_exception(
                            InternalError("Unable to deserialize child output")
                        )
                        LOGGER.exception(
                            "unable to deserialize child output", exc_info=error
                        )
            elif outcome == "timed_out":
                info.future._set_exception(TimeoutError())
            elif message.get("reason") == "request_error":
                info.future._set_exception(
                    RequestError(message="Child function request failed")
                )
            else:
                info.future._set_exception(
                    FunctionError(
                        f"Child function failed: {message.get('reason', 'function_error')}"
                    )
                )
            self._complete_future(info)

    def request_state_operation(
        self,
        operation: str,
        key: str,
        value_base64: str | None = None,
    ) -> dict[str, Any]:
        with self._condition:
            self._check_cancelled()
            operation_id = (
                f"{self.attempt_id}:state:{self._next_request_state_sequence}"
            )
            self._next_request_state_sequence += 1
            self._request_state_results[operation_id] = None

        message: dict[str, Any] = {
            "type": "request_state",
            "attempt_id": self.attempt_id,
            "operation_id": operation_id,
            "operation": {"operation": operation, "key": key},
        }
        if value_base64 is not None:
            message["operation"]["value_base64"] = value_base64
        self.runtime.protocol.write(message)

        with self._condition:
            while self._request_state_results[operation_id] is None:
                self._check_cancelled()
                LOGGER.info(
                    "attempt %s waiting for request-state operation %s",
                    self.attempt_id,
                    operation_id,
                )
                self._condition.wait(timeout=1.0)
            result = self._request_state_results.pop(operation_id)
        if result is None:
            raise InternalError("Request-state operation completed without a result")
        return result

    def deliver_request_state_result(self, message: dict[str, Any]) -> None:
        operation_id = message["operation_id"]
        with self._condition:
            if (
                operation_id not in self._request_state_results
                or self._request_state_results[operation_id] is not None
                or self.cancelled
            ):
                return
            self._request_state_results[operation_id] = message
            self._condition.notify_all()

    def run_future(self, future: Future) -> None:
        with self._runtime_hook_lock:
            self._check_cancelled()
            calls = self._register_tree(future, tail_output=None)
            self._emit_call_batch(calls)
            info = self._future_infos[self._future_durable_ids[future._id]]
            if info.future.exception is not None:
                raise info.future.exception

    def wait_futures(
        self, futures: list[Future], timeout: float | None, return_when: RETURN_WHEN
    ) -> tuple[list[Future], list[Future]]:
        with self._runtime_hook_lock:
            self._check_cancelled()
            if return_when not in (
                RETURN_WHEN.ALL_COMPLETED,
                RETURN_WHEN.FIRST_COMPLETED,
                RETURN_WHEN.FIRST_FAILURE,
            ):
                raise SDKUsageError(f"Unsupported return_when value: {return_when!r}")

            already_done = [future for future in futures if future.done()]
            if already_done and return_when != RETURN_WHEN.ALL_COMPLETED:
                return already_done, [future for future in futures if not future.done()]

            wait_deadline = None if timeout is None else time.monotonic() + timeout
            self._suspend_execution_timeout()
            try:
                for future in futures:
                    if not future.done():
                        self._watch(future, timeout)

                while True:
                    self._check_cancelled()
                    done = [future for future in futures if future.done()]
                    pending = [future for future in futures if not future.done()]
                    if return_when == RETURN_WHEN.ALL_COMPLETED and not pending:
                        return done, pending
                    if return_when == RETURN_WHEN.FIRST_COMPLETED and done:
                        return self._deterministic_winner(futures, done)
                    if return_when == RETURN_WHEN.FIRST_FAILURE:
                        failed = [
                            future for future in done if future.exception is not None
                        ]
                        if failed:
                            return self._deterministic_winner(futures, failed)
                        if not pending:
                            return done, pending

                    remaining = (
                        None
                        if wait_deadline is None
                        else wait_deadline - time.monotonic()
                    )
                    if remaining is not None and remaining <= 0:
                        for future in pending:
                            durable_id = self._future_durable_ids[future._id]
                            info = self._future_infos[durable_id]
                            future._set_exception(TimeoutError())
                            self._complete_future(info)
                        done = [future for future in futures if future.done()]
                        return done, [future for future in futures if not future.done()]
                    LOGGER.info(
                        "attempt %s waiting for %d SDK future(s)",
                        self.attempt_id,
                        len(pending),
                    )
                    with self._condition:
                        self._condition.wait(
                            timeout=1.0 if remaining is None else min(remaining, 1.0)
                        )
            finally:
                self._resume_execution_timeout()

    def await_future(self, future: Future):
        self._check_cancelled()
        durable_id = self._future_durable_ids[future._id]
        info = self._future_infos[durable_id]
        loop = asyncio.get_running_loop()
        notification = loop.create_future()

        def complete() -> None:
            if not notification.done():
                loop.call_soon_threadsafe(notification.set_result, None)

        with self._condition:
            if info.finished:
                complete()
            else:
                info.completion_callbacks.append(complete)
                self._watch(future, timeout=None)
        self._suspend_execution_timeout()
        try:
            yield from notification.__await__()
        finally:
            self._resume_execution_timeout()
        self._check_cancelled()
        future._coroutine = None

    def register_coroutine(self, coroutine: Any, future: Future) -> None:
        self._check_cancelled()
        self._coroutine_to_future[coroutine] = future

    def coroutine_to_future(self, coroutine: Any) -> Future | None:
        self._check_cancelled()
        return self._coroutine_to_future.get(coroutine)

    def _prepare_call(
        self,
    ) -> tuple[list[Any], dict[str, Any], SpecialSettings | None]:
        metadata_bytes = _b64decode(self.assignment.get("call_metadata_base64", ""))
        if not metadata_bytes:
            self._output_serializer_name = function_output_serializer(
                self.function, None
            ).name
            self._has_output_type_hint = True
            self._output_type_hint = return_type_hint(
                function_signature(self.function).return_annotation
            )
            args, kwargs = self._application_arguments()
            if self.function_instance is not None:
                set_self_arg(args, self.function_instance)
            return args, kwargs, None

        metadata = deserialize_metadata(metadata_bytes)
        if not isinstance(metadata, FunctionCallMetadata):
            raise InternalError(
                f"Expected FunctionCallMetadata, got {type(metadata).__name__}"
            )
        self._output_serializer_name = metadata.output_serializer_name_override
        self._has_output_type_hint = metadata.has_output_type_hint_override
        self._output_type_hint = metadata.output_type_hint_override
        values: dict[str, Any] = {}
        for input_value in self.assignment["inputs"]:
            value_metadata = deserialize_metadata(
                _b64decode(input_value["metadata_base64"])
            )
            if not isinstance(value_metadata, ValueMetadata):
                raise InternalError("Function argument is missing ValueMetadata")
            value = deserialize_value_with_metadata(
                _b64decode(input_value["data_base64"]), value_metadata
            )
            value_id = input_value.get("source_function_call_id") or value_metadata.id
            values[value_id] = value
        args = [values[arg.value_id] for arg in metadata.args]
        kwargs = {name: values[arg.value_id] for name, arg in metadata.kwargs.items()}
        if self.function_instance is not None:
            set_self_arg(args, self.function_instance)
        settings = None
        if (
            metadata.is_map_splitter
            or metadata.is_reduce_splitter
            or metadata.is_map_concat
        ):
            settings = SpecialSettings(
                is_map_splitter=metadata.is_map_splitter,
                is_reduce_splitter=metadata.is_reduce_splitter,
                splitter_function_name=metadata.splitter_function_name,
                splitter_input_mode=metadata.splitter_input_mode,
                is_map_concat=metadata.is_map_concat,
            )
        return args, kwargs, settings

    def _application_arguments(self) -> tuple[list[Any], dict[str, Any]]:
        inputs = self.assignment["inputs"]
        if len(inputs) != 1:
            raise InternalError(
                f"Application function call requires one HTTP payload, got {len(inputs)}"
            )
        payload = inputs[0]
        data = _b64decode(payload.get("data_base64", ""))
        content_type = payload.get("content_type", "application/octet-stream")
        parameters = list(function_signature(self.function).parameters.values())
        if not data and not parameters:
            return [], {}
        if content_type.startswith("multipart/form-data"):
            serialized_args, serialized_kwargs = _parse_multipart(data, content_type)
        else:
            serialized_args = [
                SerializedApplicationArgument(data=data, content_type=content_type)
            ]
            serialized_kwargs = {}
        return deserialize_application_function_call_arguments(
            application=self.function,
            serialized_args=serialized_args,
            serialized_kwargs=serialized_kwargs,
        )

    def _call_user_function(self, args: list[Any], kwargs: dict[str, Any]) -> Any:
        if self._special_settings is not None:
            return self._call_special_function(self._special_settings, args, kwargs)
        original = self.function._original_function
        if inspect.iscoroutinefunction(original):
            return asyncio.run(original(*args, **kwargs))
        return original(*args, **kwargs)

    def _call_special_function(
        self, settings: SpecialSettings, args: list[Any], kwargs: dict[str, Any]
    ) -> Any:
        if settings.is_map_concat:
            return args
        if settings.is_map_splitter:
            map_function = get_function(settings.splitter_function_name)
            if settings.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
                if not isinstance(args[0], list):
                    raise SDKUsageError(
                        f"Map operation input must be a list, got {type(args[0])}"
                    )
                map_inputs = args[0]
            else:
                map_inputs = args
            return self.function.future(
                *(map_function.future(item) for item in map_inputs)
            )
        if settings.is_reduce_splitter:
            reduce_function = get_function(settings.splitter_function_name)
            reduce_inputs = []
            if "initial" in kwargs:
                reduce_inputs.append(kwargs["initial"])
            if settings.splitter_input_mode == SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG:
                if not isinstance(args[0], list):
                    raise SDKUsageError(
                        f"Reduce operation input must be a list, got {type(args[0])}"
                    )
                reduce_inputs.extend(args[0])
            else:
                reduce_inputs.extend(args)
            if not reduce_inputs:
                raise SDKUsageError("reduce of empty iterable with no initial value")
            if len(reduce_inputs) == 1:
                return reduce_inputs[0]
            result = reduce_function.future(reduce_inputs[0], reduce_inputs[1])
            for item in reduce_inputs[2:]:
                result = reduce_function.future(result, item)
            return result
        raise InternalError("Invalid special Function call settings")

    def _register_tree(
        self, root: Future, tail_output: Future | None
    ) -> list[CallSpec]:
        calls: list[CallSpec] = []
        for future in dfs_bottom_up_unique_only(root):
            if future._id in self._future_durable_ids:
                if tail_output is not None:
                    raise SDKUsageError(
                        f"A tail call Future {future} is already running"
                    )
                continue
            calls.append(self._register_future(future, future is tail_output))
        return calls

    def _register_future(self, future: Future, is_tail_call: bool) -> CallSpec:
        if future._coroutine is not None and not future._run_hook_was_called:
            future._coroutine.close()
            future._coroutine = None
        future._run_hook_was_called = True
        durable_id = _future_durable_id(
            future,
            self.assignment["function_run_id"],
            self._previous_future_durable_id,
            self._future_durable_ids,
        )
        self._previous_future_durable_id = durable_id
        self._future_durable_ids[future._id] = durable_id
        self._future_infos[durable_id] = FutureInfo(future=future)

        settings = None
        if isinstance(future, FunctionCallFuture):
            function_name = future._function_name
            args = [self._resolve_argument(value) for value in future._args]
            kwargs = {
                name: self._resolve_argument(value)
                for name, value in future._kwargs.items()
            }
            if (
                is_tail_call
                and self._special_settings is not None
                and self._special_settings.is_map_splitter
            ):
                settings = SpecialSettings(is_map_concat=True)
        elif isinstance(future, MapFuture):
            function_name = self.function._name
            items = _unwrap_future(future._items)
            raw_args = items if isinstance(items, list) else [items]
            args = [self._resolve_argument(value) for value in raw_args]
            kwargs = {}
            settings = SpecialSettings(
                is_map_splitter=True,
                splitter_function_name=future._function_name,
                splitter_input_mode=(
                    SPLITTER_INPUT_MODE.ITEM_PER_ARG
                    if isinstance(items, list)
                    else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
                ),
            )
        elif isinstance(future, ReduceOperationFuture):
            function_name = self.function._name
            items = _unwrap_future(future._items)
            raw_args = items if isinstance(items, list) else [items]
            args = [self._resolve_argument(value) for value in raw_args]
            kwargs = {}
            initial = _unwrap_future(future._initial)
            if initial is not _InitialMissing:
                kwargs["initial"] = self._resolve_argument(initial)
            settings = SpecialSettings(
                is_reduce_splitter=True,
                splitter_function_name=future._function_name,
                splitter_input_mode=(
                    SPLITTER_INPUT_MODE.ITEM_PER_ARG
                    if isinstance(items, list)
                    else SPLITTER_INPUT_MODE.ITEMS_IN_ONE_ARG
                ),
            )
        else:
            raise InternalError(f"Unsupported Future type: {type(future).__name__}")
        return CallSpec(
            function_call_id=durable_id,
            function_name=function_name,
            args=args,
            kwargs=kwargs,
            is_tail_call=is_tail_call,
            special_settings=settings,
        )

    def _resolve_argument(self, value: Any) -> Any | CallReference:
        unwrapped = _unwrap_future(value)
        if isinstance(unwrapped, Future):
            return CallReference(self._future_durable_ids[unwrapped._id])
        return value

    def _emit_call_batch(self, calls: list[CallSpec]) -> None:
        if not calls:
            return
        self._check_cancelled()
        self.runtime.protocol.write(
            {
                "type": "call_batch",
                "attempt_id": self.attempt_id,
                "calls": [self._serialize_call(call) for call in calls],
            }
        )

    def _serialize_call(self, call: CallSpec) -> dict[str, Any]:
        serializer = function_input_serializer(
            get_function(call.function_name), app_call=False
        )
        inputs: list[dict[str, Any]] = []
        metadata_args: list[FunctionCallArgumentMetadata] = []
        metadata_kwargs: dict[str, FunctionCallArgumentMetadata] = {}

        def serialize_argument(value: Any, label: str) -> FunctionCallArgumentMetadata:
            if isinstance(value, CallReference):
                inputs.append(
                    {
                        "source": "function_run_output",
                        "function_call_id": value.function_call_id,
                    }
                )
                return FunctionCallArgumentMetadata(value_id=value.function_call_id)
            value_id = _stable_value_id(call.function_call_id, label)
            data, metadata = serialize_value(
                value=value,
                serializer=serializer,
                value_id=value_id,
                type_hint=type(value),
            )
            inputs.append(
                {
                    "source": "data",
                    "data_base64": _b64encode(data),
                    "metadata_base64": _b64encode(serialize_metadata(metadata)),
                    "content_type": metadata.content_type,
                }
            )
            return FunctionCallArgumentMetadata(value_id=value_id)

        for index, value in enumerate(call.args):
            metadata_args.append(serialize_argument(value, f"arg:{index}"))
        for name, value in call.kwargs.items():
            metadata_kwargs[name] = serialize_argument(value, f"kwarg:{name}")

        output_serializer_override = None
        if call.is_tail_call:
            output_serializer_override = self._output_serializer_name
        elif call.special_settings and call.special_settings.splitter_function_name:
            output_serializer_override = function_output_serializer(
                get_function(call.special_settings.splitter_function_name), None
            ).name
        settings = call.special_settings
        metadata = FunctionCallMetadata(
            id=call.function_call_id,
            function_name=call.function_name,
            output_serializer_name_override=output_serializer_override,
            output_type_hint_override=(
                self._output_type_hint if call.is_tail_call else None
            ),
            has_output_type_hint_override=(
                self._has_output_type_hint if call.is_tail_call else False
            ),
            args=metadata_args,
            kwargs=metadata_kwargs,
            is_map_splitter=settings.is_map_splitter if settings else False,
            is_reduce_splitter=settings.is_reduce_splitter if settings else False,
            splitter_function_name=(
                settings.splitter_function_name if settings else None
            ),
            splitter_input_mode=settings.splitter_input_mode if settings else None,
            is_map_concat=settings.is_map_concat if settings else False,
        )
        return {
            "function_call_id": call.function_call_id,
            "function_name": call.function_name,
            "inputs": inputs,
            "call_metadata_base64": _b64encode(serialize_metadata(metadata)),
        }

    def _watch(self, future: Future, timeout: float | None) -> None:
        durable_id = self._future_durable_ids[future._id]
        info = self._future_infos[durable_id]
        if info.watched or info.finished:
            return
        info.watched = True
        timeout_ms = None if timeout is None else max(0, int(timeout * 1000))
        self.runtime.protocol.write(
            {
                "type": "watch",
                "attempt_id": self.attempt_id,
                "function_call_id": durable_id,
                "timeout_ms": timeout_ms,
            }
        )

    def _suspend_execution_timeout(self) -> None:
        self.runtime.protocol.write({"type": "suspend", "attempt_id": self.attempt_id})

    def _resume_execution_timeout(self) -> None:
        self.runtime.protocol.write({"type": "resume", "attempt_id": self.attempt_id})

    def _complete_future(self, info: FutureInfo) -> None:
        with self._condition:
            info.finished = True
            info.completion_order = self._completion_counter
            self._completion_counter += 1
            callbacks = info.completion_callbacks
            info.completion_callbacks = []
            self._condition.notify_all()
        for callback in callbacks:
            callback()

    def _deterministic_winner(
        self, all_futures: list[Future], candidates: list[Future]
    ) -> tuple[list[Future], list[Future]]:
        winner = min(
            candidates,
            key=lambda future: self._future_infos[
                self._future_durable_ids[future._id]
            ].completion_order,
        )
        return [winner], [future for future in all_futures if future is not winner]

    def _finish_tail_call(self, output: Future) -> None:
        calls = self._register_tree(output, tail_output=output)
        self._emit_call_batch(calls)
        self._check_cancelled()
        self.runtime.protocol.write(
            {
                "type": "success",
                "attempt_id": self.attempt_id,
                "result": {
                    "type": "call_graph",
                    "output_function_call_id": self._future_durable_ids[output._id],
                },
            }
        )

    def _finish_value(self, output: Any) -> None:
        serializer = function_output_serializer(
            self.function, self._output_serializer_name
        )
        data, metadata = serialize_value(
            value=output,
            serializer=serializer,
            value_id=_stable_value_id(self.assignment["function_run_id"], "output"),
            type_hint=(
                self._output_type_hint if self._has_output_type_hint else type(output)
            ),
        )
        self._check_cancelled()
        self.runtime.protocol.write(
            {
                "type": "success",
                "attempt_id": self.attempt_id,
                "result": {
                    "type": "value",
                    "output_base64": _b64encode(data),
                    "metadata_base64": _b64encode(serialize_metadata(metadata)),
                    "content_type": metadata.content_type,
                },
            }
        )

    def _failure(self, error: BaseException) -> None:
        if isinstance(error, InternalError):
            reason = "internal_error"
        elif isinstance(error, RequestError):
            reason = "request_error"
        elif isinstance(error, TimeoutError):
            reason = "function_timeout"
        else:
            reason = "function_error"
        LOGGER.exception("attempt %s failed", self.attempt_id, exc_info=error)
        self.runtime.protocol.write(
            {
                "type": "failure",
                "attempt_id": self.attempt_id,
                "reason": reason,
                "message": f"{type(error).__name__}: {error}",
            }
        )

    def _check_cancelled(self) -> None:
        if self.cancelled:
            raise RunnerCancelled()


class PythonFunctionRunner:
    def __init__(self, protocol: ProtocolWriter):
        self.protocol = protocol
        self._lock = threading.Lock()
        self._attempts: dict[str, Attempt] = {}
        self._initialized = False
        self._protocol_initialized = False
        self._code_sha256: str | None = None
        self._function_name: str | None = None
        self._function: Any = None
        self._function_instance: Any | None = None
        self._code_zip_path: str | None = None
        self._install_runtime_hooks()

    async def serve(self, core: FunctionAgentCore) -> None:
        while True:
            try:
                message = json.loads(await core.next_input())
                if message.get("type") == "shutdown":
                    return
                await asyncio.to_thread(self._handle_message, message)
            except asyncio.CancelledError:
                raise
            except BaseException as error:
                LOGGER.exception("invalid function-agent input", exc_info=error)
                raise

    def _handle_message(self, message: dict[str, Any]) -> None:
        message_type = message["type"]
        if message_type == "assignment":
            self._assignment(message["assignment"])
        elif message_type == "call_result":
            self._call_result(message)
        elif message_type == "request_state_result":
            self._request_state_result(message["result"])
        elif message_type == "cancel":
            self._cancel(message["attempt_id"])
        else:
            raise ValueError(f"Unknown function-agent input type {message_type!r}")

    def remove_attempt(self, attempt_id: str) -> None:
        with self._lock:
            self._attempts.pop(attempt_id, None)

    def _assignment(self, assignment: dict[str, Any]) -> None:
        attempt_id = assignment["attempt_id"]
        try:
            _apply_resolved_environment(assignment)
            if not self._initialized:
                self._initialize(assignment)
            if not self._protocol_initialized:
                self.protocol.write({"type": "initialized"})
                self._protocol_initialized = True
            self._validate_assignment(assignment)
            attempt = Attempt(
                runtime=self,
                assignment=assignment,
                function=self._function,
                function_instance=self._function_instance,
            )
            with self._lock:
                if attempt_id in self._attempts:
                    return
                self._attempts[attempt_id] = attempt
            threading.Thread(target=attempt.run, daemon=True).start()
        except BaseException as error:
            if not self._protocol_initialized:
                self.protocol.write({"type": "initialized"})
                self._protocol_initialized = True
            self.protocol.write(
                {
                    "type": "failure",
                    "attempt_id": attempt_id,
                    "reason": "function_error",
                    "message": f"{type(error).__name__}: {error}",
                }
            )
            LOGGER.exception(
                "Python function runner initialization failed", exc_info=error
            )

    def _initialize(self, assignment: dict[str, Any]) -> None:
        code = _b64decode(assignment["application_code_base64"])
        digest = hashlib.sha256(code).hexdigest()
        if digest != assignment["application_code_sha256"]:
            raise ValueError("Application code SHA-256 does not match assignment")
        descriptor, path = tempfile.mkstemp(suffix=".zip")
        with os.fdopen(descriptor, "wb") as code_file:
            code_file.write(code)
        sys.path.insert(0, path)
        with zipfile.ZipFile(path, "r") as archive:
            with archive.open(CODE_ZIP_MANIFEST_FILE_NAME) as manifest_file:
                manifest = CodeZIPManifest.model_validate(json.load(manifest_file))
        function_name = assignment["function"]
        if function_name not in manifest.functions:
            raise ValueError(
                f"Function {function_name!r} not found; available: {list(manifest.functions)}"
            )
        importlib.import_module(manifest.functions[function_name].module_import_name)
        if not has_function(function_name):
            raise ValueError(
                f"Function {function_name!r} was not registered; available: {get_functions()!r}"
            )
        function = get_function(function_name)
        function_instance = None
        if function._function_config.class_name is not None:
            function_instance = create_self_instance(
                function._function_config.class_name
            )
        setup_multiprocessing()
        self._code_zip_path = path
        self._code_sha256 = digest
        self._function_name = function_name
        self._function = function
        self._function_instance = function_instance
        self._initialized = True

    def _validate_assignment(self, assignment: dict[str, Any]) -> None:
        if assignment["application_code_sha256"] != self._code_sha256:
            raise ValueError(
                "Persistent Python runner received different application code"
            )
        if assignment["function"] != self._function_name:
            raise ValueError("Persistent Python runner received a different function")

    def _call_result(self, message: dict[str, Any]) -> None:
        with self._lock:
            attempt = self._attempts.get(message["attempt_id"])
        if attempt is not None:
            attempt.deliver_result(message)

    def _request_state_result(self, message: dict[str, Any]) -> None:
        with self._lock:
            attempt = self._attempts.get(message["attempt_id"])
        if attempt is not None:
            attempt.deliver_request_state_result(message)

    def _cancel(self, attempt_id: str) -> None:
        with self._lock:
            attempt = self._attempts.get(attempt_id)
        if attempt is not None:
            attempt.cancel()

    def _install_runtime_hooks(self) -> None:
        global _RUNTIME_HOOKS_INSTALLED
        with _RUNTIME_HOOKS_LOCK:
            if _RUNTIME_HOOKS_INSTALLED:
                return
            set_run_future_hook(
                lambda future: _CURRENT_ATTEMPT.get().run_future(future)
            )
            set_wait_futures_hook(
                lambda futures, timeout, return_when: _CURRENT_ATTEMPT.get().wait_futures(
                    futures, timeout, return_when
                )
            )
            set_await_future_hook(
                lambda future: _CURRENT_ATTEMPT.get().await_future(future)
            )
            set_register_coroutine_hook(
                lambda coroutine, future: _CURRENT_ATTEMPT.get().register_coroutine(
                    coroutine, future
                )
            )
            set_coroutine_to_future_hook(
                lambda coroutine: _CURRENT_ATTEMPT.get().coroutine_to_future(coroutine)
            )
            _RUNTIME_HOOKS_INSTALLED = True


def _future_durable_id(
    future: Future,
    parent_function_call_id: str,
    previous_future_durable_id: str,
    durable_ids: dict[str, str],
) -> str:
    attributes = [parent_function_call_id, previous_future_durable_id]
    if isinstance(future, FunctionCallFuture):
        attributes.extend(["FunctionCall", future._function_name])
        for value in future._args:
            _add_future_id(value, durable_ids, attributes)
        for name in sorted(future._kwargs):
            _add_future_id(future._kwargs[name], durable_ids, attributes)
    elif isinstance(future, MapFuture):
        attributes.append(f"MAP_OPERATION:{future._function_name}")
        items = _unwrap_future(future._items)
        for value in items if isinstance(items, list) else [items]:
            _add_future_id(value, durable_ids, attributes)
    elif isinstance(future, ReduceOperationFuture):
        attributes.extend(["ReduceOperation", future._function_name])
        _add_future_id(future._initial, durable_ids, attributes)
        items = _unwrap_future(future._items)
        for value in items if isinstance(items, list) else [items]:
            _add_future_id(value, durable_ids, attributes)
    else:
        raise InternalError(f"Unsupported Future type: {type(future).__name__}")
    digest = hashlib.sha256()
    for attribute in attributes:
        encoded = attribute.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        digest.update(b"|")
    return digest.hexdigest()


def _add_future_id(value: Any, durable_ids: dict[str, str], output: list[str]) -> None:
    value = _unwrap_future(value)
    if isinstance(value, Future):
        durable_id = durable_ids.get(value._id)
        if durable_id is None:
            raise InternalError(f"Durable ID for Future {value._id} is unavailable")
        output.append(durable_id)


def _stable_value_id(function_call_id: str, label: str) -> str:
    return hashlib.sha256(f"{function_call_id}\0{label}".encode()).hexdigest()


def _parse_multipart(
    body: bytes, content_type: str
) -> tuple[
    list[SerializedApplicationArgument], dict[str, SerializedApplicationArgument]
]:
    message = BytesParser(policy=email_policy).parsebytes(
        f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode() + body
    )
    if not message.is_multipart():
        raise SDKUsageError("Malformed multipart application request")
    positional: dict[int, SerializedApplicationArgument] = {}
    keyword: dict[str, SerializedApplicationArgument] = {}
    for part in message.iter_parts():
        name = part.get_param("name", header="content-disposition")
        if name is None:
            continue
        argument = SerializedApplicationArgument(
            data=part.get_payload(decode=True) or b"",
            content_type=part.get("content-type", "application/octet-stream"),
        )
        try:
            positional[int(name)] = argument
        except ValueError:
            keyword[name] = argument
    args = []
    first_index = 0 if 0 in positional else 1
    for index in range(first_index, first_index + len(positional)):
        if index not in positional:
            raise SDKUsageError(f"Missing positional multipart argument {index}")
        args.append(positional[index])
    return args, keyword


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="tensorlake-python-function-runner",
        description="Tensorlake Python runner with an embedded Rust function agent",
    )
    parser.add_argument("--function-service-url", required=True)
    parser.add_argument("--registration-token", required=True)
    parser.add_argument("--agent-id")
    parser.add_argument("--incarnation")
    parser.add_argument("--secret-service-workload-url")
    parser.add_argument("--credential-request-timeout-ms", type=int, default=10_000)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> None:
    core = FunctionAgentCore(
        args.function_service_url,
        args.registration_token,
        agent_id=args.agent_id,
        incarnation=args.incarnation,
        secret_service_workload_url=args.secret_service_workload_url,
        credential_request_timeout_ms=args.credential_request_timeout_ms,
    )
    protocol = ProtocolWriter(core, asyncio.get_running_loop())
    await PythonFunctionRunner(protocol).serve(core)


def main() -> None:
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(_run(_parse_args()))


if __name__ == "__main__":
    main()
