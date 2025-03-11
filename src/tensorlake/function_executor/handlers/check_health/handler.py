import os
import subprocess
import time
from typing import Any

from ...proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
)


class Handler:
    def __init__(self, logger: Any):
        self._logger: Any = logger.bind(module=__name__)
        self._enable_gpu_health_checks = _enable_gpu_health_checks()
        self._logged_gpu_health_check_failure = False

        if self._enable_gpu_health_checks:
            self._logger.info("enabling GPU health checks")

    def run(self, request: HealthCheckRequest) -> HealthCheckResponse:
        # This health check validates that the Server:
        # - Has its process alive (not exited).
        # - Didn't exhaust its thread pool.
        # - Is able to communicate over its server socket.
        # - If NVIDIA GPUs are available then verify that they are working okay.
        if self._enable_gpu_health_checks:
            return self._gpu_health_check()
        else:
            return HealthCheckResponse(
                healthy=True, status_message="Function Executor gRPC channel is healthy"
            )

    def _gpu_health_check(self) -> HealthCheckResponse:
        start_time = time.monotonic()
        result: subprocess.CompletedProcess = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
        )
        duration = time.monotonic() - start_time

        if result.returncode == 0:
            return HealthCheckResponse(
                healthy=True,
                status_message="Function Executor gRPC channel is healthy and nvidia-smi completes successfully",
            )

        # Only log this error once to avoid log spam.
        if not self._logged_gpu_health_check_failure:
            self._logged_gpu_health_check_failure = True
            self._logger.error(
                "NVIDIA GPU health check failed.",
                nvidia_smi_output=result.stdout,
                nvidia_smi_error=result.stderr,
                nvidia_smi_return_code=result.returncode,
                nvidia_smi_duration_sec=f"{duration:.3f}",
            )
        return HealthCheckResponse(
            healthy=False,
            status_message="Function Executor gRPC channel is healthy but nvidia-smi fails",
        )


def _enable_gpu_health_checks() -> bool:
    # NVIDIA_VISIBLE_DEVICES is set by NVIDIA Docker runtime when GPUs are provided.
    # nvidia-smi is installed with NVIDIA GPU drivers.
    # If both are available then run health checks to detect if the Function Executor
    # is currently affected by known issue https://github.com/NVIDIA/nvidia-container-toolkit/issues/857.
    if "NVIDIA_VISIBLE_DEVICES" not in os.environ:
        return False

    result: subprocess.CompletedProcess = subprocess.run(["which", "nvidia-smi"])
    return result.returncode == 0  # Enable the health check if nvidia-smi is available
