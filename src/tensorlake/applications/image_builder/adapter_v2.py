"""Adapter to wrap ImageBuilderV2Client with BuildRequest interface."""

from __future__ import annotations

import os

import click

from tensorlake.applications.image_builder import BuildRequest

from .client_v2 import BuildContext, ImageBuilderV2Client
from .exceptions import ImageBuilderV2BuildError, ImageBuilderV2NetworkError


class ImageBuilderV2Adapter:
    """Wraps ImageBuilderV2Client to accept BuildRequest and match v3's interface."""

    def __init__(self, client: ImageBuilderV2Client):
        """
        Initialize the v2 adapter.

        Args:
            client: The v2 client to wrap
        """
        self._client = client

    @classmethod
    def from_context(
        cls,
        api_key: str | None = None,
        pat: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
    ) -> ImageBuilderV2Adapter:
        """
        Create adapter from authentication context.

        Args:
            api_key: API key for authentication
            pat: Personal access token for authentication
            organization_id: Organization ID
            project_id: Project ID

        Returns:
            ImageBuilderV2Adapter instance
        """
        # Use PAT if API key not provided
        auth_token = api_key or pat

        # Get build service URL
        server_url = os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai")
        build_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v2")

        # Create v2 client
        client = ImageBuilderV2Client(
            build_service=build_url,
            api_key=auth_token,
            organization_id=organization_id,
            project_id=project_id,
        )

        return cls(client=client)

    async def build(self, req: BuildRequest) -> None:
        """
        Build images using v2 client (sequential).

        Converts BuildRequest to multiple BuildContext calls.

        Args:
            req: BuildRequest containing images to build

        Raises:
            ImageBuilderV2BuildError: On build failures
            ImageBuilderV2NetworkError: On network errors
        """
        # Display version indicator
        click.echo("🔧 Using ImageBuilder v2")
        click.echo()

        try:
            # Build each image sequentially (v2 limitation)
            for image_req in req.images:
                image = image_req.image_info.image

                # Get the first function name for the build context
                # v2 builds one function at a time
                for func_info in image_req.image_info.functions:
                    context = BuildContext(
                        application_name=req.name,
                        application_version=req.version,
                        function_name=func_info.function_name,
                    )

                    try:
                        await self._client.build(context, image)
                    except RuntimeError as e:
                        # Wrap v2 RuntimeError in unified exception hierarchy
                        raise ImageBuilderV2BuildError(
                            f"Build failed for {image.name}: {e}"
                        ) from e
                    except Exception as e:
                        # Wrap network/other errors
                        error_msg = str(e)
                        if "network" in error_msg.lower() or "connection" in error_msg.lower():
                            raise ImageBuilderV2NetworkError(
                                f"Network error building {image.name}: {e}"
                            ) from e
                        else:
                            raise ImageBuilderV2BuildError(
                                f"Error building {image.name}: {e}"
                            ) from e

        except (ImageBuilderV2BuildError, ImageBuilderV2NetworkError):
            # Re-raise our wrapped exceptions
            raise
        except Exception as e:
            # Catch any other unexpected errors
            raise ImageBuilderV2BuildError(
                f"Unexpected error during build: {e}"
            ) from e
