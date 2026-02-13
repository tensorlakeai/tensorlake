"""Protocol definition for ImageBuilder interface."""

from __future__ import annotations

from typing import Protocol

from .build_request import BuildRequest


class ImageBuilder(Protocol):
    """Protocol defining the common interface for ImageBuilder implementations."""

    async def build(self, req: BuildRequest) -> None:
        """
        Build images according to the build request.

        Args:
            req: BuildRequest containing images to build and configuration

        Raises:
            ImageBuilderError: On build failures
        """
        ...
