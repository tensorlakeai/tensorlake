import importlib
import json
import sys
import tempfile
import time
import zipfile
from typing import Any, Generator, Iterator, Optional

import grpc

from tensorlake.functions_sdk.functions import TensorlakeFunctionWrapper
from tensorlake.functions_sdk.graph_definition import ComputeGraphMetadata
from tensorlake.functions_sdk.graph_serialization import (
    GRAPH_MANIFEST_FILE_NAME,
    GRAPH_METADATA_FILE_NAME,
    FunctionManifest,
    GraphManifest,
)

from .handlers.check_health.handler import Handler as CheckHealthHandler
from .handlers.run_function.handler import Handler as RunTaskHandler
from .handlers.run_function.request_validator import (
    RequestValidator as RunTaskRequestValidator,
)
from .info import info_response_kv_args
from .initialize_request_validator import InitializeRequestValidator
from .invocation_state.invocation_state_proxy_server import InvocationStateProxyServer
from .invocation_state.proxied_invocation_state import ProxiedInvocationState
from .proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
    InfoRequest,
    InfoResponse,
    InitializationFailureReason,
    InitializationOutcomeCode,
    InitializeRequest,
    InitializeResponse,
    InvocationStateRequest,
    InvocationStateResponse,
    RunTaskRequest,
    RunTaskResponse,
)
from .proto.function_executor_pb2_grpc import FunctionExecutorServicer


class Service(FunctionExecutorServicer):
    def __init__(self, logger: Any):
        self._logger = logger.bind(module=__name__, **info_response_kv_args())
        self._namespace: Optional[str] = None
        self._graph_name: Optional[str] = None
        self._graph_version: Optional[str] = None
        self._function_name: Optional[str] = None
        self._function_wrapper: Optional[TensorlakeFunctionWrapper] = None
        self._graph_metadata: Optional[ComputeGraphMetadata] = None
        self._invocation_state_proxy_server: Optional[InvocationStateProxyServer] = None
        self._check_health_handler = CheckHealthHandler(self._logger)

    def initialize(
        self, request: InitializeRequest, context: grpc.ServicerContext
    ) -> InitializeResponse:
        start_time = time.monotonic()
        self._logger.info("initializing function executor service")

        request_validator: InitializeRequestValidator = InitializeRequestValidator(
            request
        )
        request_validator.check()

        self._namespace = request.namespace
        self._graph_name = request.graph_name
        self._graph_version = request.graph_version
        self._function_name = request.function_name
        # The function is only loaded once per Function Executor. It's important to use a single
        # loaded function so all the tasks when executed are sharing the same memory. This allows
        # implementing smart caching in customer code. E.g. load a model into GPU only once and
        # share the model's file descriptor between all tasks or download function configuration
        # only once.
        self._logger = self._logger.bind(
            namespace=request.namespace,
            graph=request.graph_name,
            graph_version=request.graph_version,
            fn=request.function_name,
        )

        graph_modules_zip_fd, graph_modules_zip_path = tempfile.mkstemp(suffix=".zip")
        with open(graph_modules_zip_fd, "wb") as graph_modules_zip_file:
            graph_modules_zip_file.write(request.graph.data)
        sys.path.insert(
            0, graph_modules_zip_path
        )  # Add as the first entry so user modules have highest priority

        try:
            # Process user controlled input in a try-except block to not treat errors here as our
            # internal platform errors.
            with zipfile.ZipFile(graph_modules_zip_path, "r") as zf:
                with zf.open(GRAPH_MANIFEST_FILE_NAME) as graph_manifest_file:
                    graph_manifest: GraphManifest = GraphManifest.model_validate(
                        json.load(graph_manifest_file)
                    )
                with zf.open(GRAPH_METADATA_FILE_NAME) as graph_metadata_file:
                    self._graph_metadata: ComputeGraphMetadata = (
                        ComputeGraphMetadata.model_validate(
                            json.load(graph_metadata_file)
                        )
                    )
            if request.function_name not in graph_manifest.functions:
                raise ValueError(
                    f"Function {request.function_name} is not defined in the graph manifest {graph_manifest}"
                )

            function_manifest: FunctionManifest = graph_manifest.functions[
                request.function_name
            ]
            function_module = importlib.import_module(
                function_manifest.module_import_name
            )
            function_class = getattr(
                function_module, function_manifest.class_import_name
            )

            # TODO: capture stdout and stderr the same way as when we run tasks.
            self._function_wrapper = TensorlakeFunctionWrapper(function_class)
        except Exception as e:
            self._logger.error(
                "function executor service initialization failed",
                reason="failed to load customer function",
                duration_sec=f"{time.monotonic() - start_time:.3f}",
                exc_info=e,
            )
            return InitializeResponse(
                outcome_code=InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_FAILURE,
                failure_reason=InitializationFailureReason.INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR,
                stderr=str(e),
            )

        self._logger.info(
            "initialized function executor service",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        return InitializeResponse(
            outcome_code=InitializationOutcomeCode.INITIALIZE_OUTCOME_CODE_SUCCESS
        )

    def initialize_invocation_state_server(
        self,
        client_responses: Iterator[InvocationStateResponse],
        context: grpc.ServicerContext,
    ) -> Generator[InvocationStateRequest, None, None]:
        start_time = time.monotonic()
        self._logger.info("initializing invocation proxy server")

        if self._invocation_state_proxy_server is not None:
            self._logger.error(
                "invocation state proxy server already exists, looks like client reconnected without disconnecting first"
            )
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "invocation state proxy server already exists, please disconnect first",
            )

        self._invocation_state_proxy_server = InvocationStateProxyServer(
            client_responses, self._logger
        )

        self._logger.info(
            "initialized invocation proxy server",
            duration_sec=f"{time.monotonic() - start_time:.3f}",
        )
        yield from self._invocation_state_proxy_server.run()
        self._invocation_state_proxy_server = None

    def run_task(
        self, request: RunTaskRequest, context: grpc.ServicerContext
    ) -> RunTaskResponse:
        # Customer function code never raises an exception because we catch all of them and add
        # their details to the response. We can only get an exception here if our own code failed.
        # If our code raises an exception the grpc framework converts it into GRPC_STATUS_UNKNOWN
        # error with the exception message. Differentiating errors is not needed for now.
        RunTaskRequestValidator(request=request).check()
        self._check_task_routed_correctly(request)

        return RunTaskHandler(
            request=request,
            graph_name=self._graph_name,
            graph_version=self._graph_version,
            function_name=self._function_name,
            invocation_state=ProxiedInvocationState(
                request.task_id, self._invocation_state_proxy_server
            ),
            function_wrapper=self._function_wrapper,
            graph_metadata=self._graph_metadata,
            logger=self._logger,
        ).run()

    def _check_task_routed_correctly(self, request: RunTaskRequest):
        # Fail with internal error as this happened due to wrong task routing to this Server.
        # If we run the wrongly routed task then it can steal data from this Server if it belongs
        # to a different customer.
        if request.namespace != self._namespace:
            raise ValueError(
                f"This Function Executor is not initialized for this namespace {request.namespace}"
            )
        if request.graph_name != self._graph_name:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_name {request.graph_name}"
            )
        if request.graph_version != self._graph_version:
            raise ValueError(
                f"This Function Executor is not initialized for this graph_version {request.graph_version}"
            )
        if request.function_name != self._function_name:
            raise ValueError(
                f"This Function Executor is not initialized for this function_name {request.function_name}"
            )

    def check_health(
        self, request: HealthCheckRequest, context: grpc.ServicerContext
    ) -> HealthCheckResponse:
        return self._check_health_handler.run(request)

    def get_info(
        self, request: InfoRequest, context: grpc.ServicerContext
    ) -> InfoResponse:
        return InfoResponse(**info_response_kv_args())
