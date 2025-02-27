import os
import subprocess
from typing import Any, Dict, Optional

from ...proto.function_executor_pb2 import (
    HealthCheckRequest,
    HealthCheckResponse,
)


class CheckHealthHandler:
    """Stateful handler for health checks."""

    def __init__(self):
        self._enable_gpu_health_checks = False
        self._logged_gpu_health_check_failure = False
        self._torch: Optional[Any] = None

    def initialize(self, logger: Any) -> None:
        if not _gpu_is_availbale():
            return

        self._enable_gpu_health_checks = True
        logger = logger.bind(module=__name__)
        logger.info("enabling GPU health checks")

        try:
            import torch

            self._torch = torch
        except ImportError as e:
            logger.info("torch is not available for the health check", exc_info=str(e))

    def handle(self, request: HealthCheckRequest, logger: Any) -> HealthCheckResponse:
        # This health check validates that the Server:
        # - Has its process alive (not exited).
        # - Didn't exhaust its thread pool.
        # - Is able to communicate over its server socket.
        # - If NVIDIA GPUs are available then verify that they are working okay.
        if self._enable_gpu_health_checks:
            logger = logger.bind(module=__name__)
            return self._gpu_health_check(logger)
        else:
            return HealthCheckResponse(healthy=True)

    def _gpu_health_check(self, logger) -> HealthCheckResponse:
        result: subprocess.CompletedProcess = subprocess.run(
            ["nvidia-smi"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self._log_gpu_health_check_failure_once(
                {
                    "reason": "nvidia-smi failed",
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
                logger,
            )
            return HealthCheckResponse(healthy=False)

        if self._torch is None:
            return HealthCheckResponse(healthy=True)

        try:
            # Run a simple CUDA computation to verify that the GPU is working.
            gpu_device = self._torch.device("cuda:0")
            x = self._torch.tensor([[1, 2, 3], [4, 5, 6]]).to(gpu_device)
            y = self._torch([[7, 8, 9], [10, 11, 12]]).to(gpu_device)
            x + y
        except Exception as e:
            self._log_gpu_health_check_failure_once(
                {
                    "reason": "torch CUDA computation failed",
                    "exc_info": str(e),
                },
                logger,
            )
            return HealthCheckResponse(healthy=False)

        return HealthCheckResponse(healthy=True)

    def _log_gpu_health_check_failure_once(
        self, labels: Dict[str, str], logger: Any
    ) -> None:
        if self._logged_gpu_health_check_failure:
            return

        self._logged_gpu_health_check_failure = True
        logger.error(
            "NVIDIA GPU health check failed.",
            **labels,
        )


def _gpu_is_availbale() -> bool:
    # NVIDIA_VISIBLE_DEVICES is set by NVIDIA Docker runtime when GPUs are provided.
    # nvidia-smi is installed with NVIDIA GPU drivers.
    # If both are available then run health checks to detect that the Function Executor
    # is currently affected by known issue https://github.com/NVIDIA/nvidia-container-toolkit/issues/857.
    return (
        "NVIDIA_VISIBLE_DEVICES" in os.environ and os.system("which -s nvidia-smi") == 0
    )
