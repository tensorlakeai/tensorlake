"""Validate live targets without rounding; preserve create's integral numeric coercion."""

import math

from .exceptions import SandboxError
from .models import ResizeSandboxResources


def resize_resources(
    cpus: float | None, memory_mb: int | None, disk_mb: int | None
) -> ResizeSandboxResources | None:
    if cpus is not None:
        if (
            isinstance(cpus, bool)
            or not isinstance(cpus, (int, float))
            or not math.isfinite(cpus)
            or cpus <= 0
            or cpus % 1 != 0
        ):
            raise SandboxError(
                f"cpus {cpus!r} must be a finite positive whole-vCPU count"
            )
    for field, value in (("memory_mb", memory_mb), ("disk_mb", disk_mb)):
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or (isinstance(value, float) and not math.isfinite(value))
            or value <= 0
            or value % 1 != 0
        ):
            raise SandboxError(
                f"{field} {value!r} must be a positive integer number of MiB"
            )
    if cpus is None and memory_mb is None and disk_mb is None:
        return None
    return ResizeSandboxResources(cpus=cpus, memory_mb=memory_mb, disk_mb=disk_mb)


def validate_resize_wait(
    timeout: float, poll_interval: float, generation: int | None = None
) -> None:
    for field, value, positive in (
        ("timeout", timeout, False),
        ("poll_interval", poll_interval, True),
    ):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            or (positive and value == 0)
        ):
            raise SandboxError(
                f"{field} {value!r} must be finite and {'positive' if positive else 'non-negative'}"
            )
    if generation is not None and (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise SandboxError(f"generation {generation!r} must be a positive integer")
