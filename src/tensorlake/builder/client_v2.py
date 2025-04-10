"""
# TensorLake Image Builder Client
# This code is part of the TensorLake SDK for Python.
# It provides a client for interacting with the image builder service.

# This client is a new revision of the client found in `src/tensorlake/builder/client.py`.
# It is designed to interact with the /images/v2 API endpoint.
# The client allows users to build images, check the status of builds,
# and stream logs from the image builder service.

# The client is initialized with the build service URL and an optional API key.
# The API key is used for authentication when making requests to the service.
# The client provides methods to get build information, check if a build exists,
# and stream logs from a build.

# The client uses the httpx library for making asynchronous HTTP requests.
# The client is designed to be used in an asynchronous context, allowing for
# non-blocking operations when interacting with the image builder service.

# The client is initialized with the build service URL and an optional API key.
"""

import os
import tempfile
from dataclasses import dataclass

import click
import httpx
from httpx_sse import aconnect_sse
from pydantic import BaseModel
from typing import Dict

from tensorlake import Image


@dataclass
class BuildContext:
    """
    Build context for the image builder service.
    This context contains information about the graph, graph version,
    and function name used for building the image.

    Attributes:
        graph (str): The name of the graph to be built.
        graph_version (str): The version of the graph to be built.
        function_name (str): The name of the function used in the build.

    Example:
        context = BuildContext(
            graph="example_graph",
            graph_version="v1.0",
            function_name="example_function"
        )
    """

    graph: str
    graph_version: str
    function_name: str


class NewBuild(BaseModel):
    """
    NewBuild model for the image builder service.
    This model represents the response from the image builder service
    when a new build is created.
    Attributes:
        id (str): The ID of the new build. Has prefix "build_".
    """

    build_id: str

class BuildLogEvent(BaseModel):
    """
    BuildLogEvent model for the image builder service.
    This model represents a log event from the image builder service.
    Attributes:
        event (str): The type of event (e.g., "login").
        data (dict): The data associated with the event.
    """
    build_id: str
    timestamp: str
    stream: str
    message: str
    sequence_number: int

class ImageBuilderV2Client:
    """
    Client for interacting with the image builder service.
    This client is used to build images, check the status of builds,
    and stream logs from the image builder service.
    """

    def __init__(self, build_service: str, api_key):
        self._client = httpx.AsyncClient()
        self._build_service = build_service
        self._headers = {}
        if api_key:
            self._headers["Authorization"] = f"Bearer {api_key}"

    @classmethod
    def from_env(cls):
        """
        Create an instance of the ImageBuilderV2Client using environment variables.

        The API key is retrieved from the TENSORLAKE_API_KEY environment variable.
        The build service URL is retrieved from the INDEXIFY_URL environment variable,
        defaulting to "https://api.tensorlake.ai" if not set.

        The TENSORLAKE_BUILD_SERVICE environment variable can be used to specify
        a different build service URL, mainly for debugging or local testing.

        Returns:
            ImageBuilderV2Client: An instance of the ImageBuilderV2Client.
        """
        api_key = os.getenv("TENSORLAKE_API_KEY")
        server_url = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")
        build_url = os.getenv("TENSORLAKE_BUILD_SERVICE", f"{server_url}/images/v2")
        return cls(build_url, api_key)

    async def build_collection(self, context_collection: Dict[Image, BuildContext]) -> Dict[str, str]:
        """
        Build a collection of images using the provided build context.

        Args:
            context_collection (Dict[Image, BuildContext]): A dictionary mapping images to their build contexts.
        Returns:
            dict: A dictionary mapping image hashes to their corresponding build IDs.
        """
        click.echo("Building images...")

        builds = {}
        for image, context in context_collection.items():
            click.echo(f"Building {image.name()}")
            build = await self.build(context, image)
            click.echo(f"Built {image.name()} with hash {image.hash}")
            builds[image.hash] = build.build_id

        return builds

    async def build(self, context: BuildContext, image: Image) -> NewBuild:
        """
        Build an image using the provided build context.

        Args:
            context (BuildContext): The build context containing information about the graph,
                                    graph version, and function name.
            image (Image): The image to be built.
        Returns:
            dict: The response from the image builder service.
        """
        click.echo(
            f"Building {context.graph} version {context.graph_version} for {context.function_name}"
        )

        _fd, context_file = tempfile.mkstemp()
        image.build_context(context_file)

        click.echo(
            f"{context.graph}: Posting {os.path.getsize(context_file)} bytes of context to build service...."
        )
        files = {"context": open(context_file, "rb")}
        data = {
            "graph_name": context.graph,
            "graph_version": context.graph_version,
            "graph_function_name": context.function_name,
            "image_hash": image.hash,
        }

        res = await self._client.post(
            f"{self._build_service}/builds",
            data=data,
            files=files,
            headers=self._headers,
            timeout=60,
        )

        res.raise_for_status()
        build = NewBuild.model_validate(res.json())

        click.secho(f"Build ID: {build.build_id}", fg="green")
        return build

    async def stream_logs(self, build: NewBuild):
        """
        Stream logs from the image builder service for the specified build.

        Args:
            build (NewBuild): The build for which to stream logs.
        """
        click.echo(f"Streaming logs for build {build.build_id}")

        async with httpx.AsyncClient() as client:
            async with aconnect_sse(
                client,
                "GET",
                f"{self._build_service}/builds/{build.build_id}/logs",
                headers=self._headers,
            ) as event_source:
                events = [sse async for sse in event_source.aiter_sse()]
                (sse,) = events

                log_entry = BuildLogEvent.model_validate(sse.json())
                match log_entry.stream:
                    case "stdout":
                        click.echo(log_entry.message)
                    case "stderr":
                        click.secho(log_entry.message, fg="red")
                    case "info":
                        click.secho(f"{log_entry.timestamp}: {log_entry.message}", fg="blue")
