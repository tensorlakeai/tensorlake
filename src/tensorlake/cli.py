import asyncio
import importlib
import inspect
import os
import pathlib
import sys
import tempfile
import time
from typing import Dict, List

import click
import httpx

from tensorlake import Graph, Image, RemoteGraph, TensorlakeClient
from tensorlake.builder.client import ImageBuilderClient
from tensorlake.functions_sdk.image import Build


@click.group()
def tensorlake():
    pass


@click.command()
@click.option("-p", "--parallel-builds", is_flag=True, default=False)
@click.option("-r", "--retry", is_flag=True, default=False)
@click.argument("workflow_file", type=click.File("r"))
def deploy(workflow_file: click.File, parallel_builds: bool, retry: bool):
    """Deploy a workflow to tensorlake."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder = ImageBuilderClient.from_env()
    seen_images: Dict[Image, str] = {}
    deployed_graphs: List[Graph] = []

    workflow = _import_workflow_file(workflow_file.name)
    for name in dir(workflow):
        obj = getattr(workflow, name)
        if isinstance(obj, Graph):
            deployed_graphs.append(obj)
            for node_name, node_obj in obj.nodes.items():
                image = node_obj.image
                if image is None:
                    raise click.ClickException(
                        f"graph function {node_name} needs to use an image"
                    )
                if image in seen_images:
                    continue
                seen_images[image] = image.hash()

    asyncio.run(
        _prepare_images(
            builder, seen_images, parallel_builds=parallel_builds, retry=retry
        )
    )

    # If we are still here then our images should all have URIs
    project_id = _get_project_id()
    client = TensorlakeClient(namespace=project_id)
    click.secho("Everything looks good, deploying now", fg="green")
    for graph in deployed_graphs:
        # TODO: Every time we post we get a new version, is that expected or the client should do the checks?
        remote = RemoteGraph.deploy(graph, client=client)
        click.secho(f"Deployed {graph.name}", fg="green")


def _stream_build_log(builder: ImageBuilderClient, build: Build, print_logs=True):
    with builder.client.stream(
        "GET",
        f"{builder.build_service}/v1/builds/{build.id}/log",
        timeout=500,
        headers=builder.headers,
    ) as r:
        for line in r.iter_lines():
            if print_logs:
                print(line)


async def _wait_for_build(builder: ImageBuilderClient, build: Build, print_logs=True):
    click.echo(f"Waiting for {build.image_name} to start building")
    while build.status == "ready":
        await asyncio.sleep(5)
        build = builder.get_build(build.id)

    # Start streaming logs
    await asyncio.to_thread(_stream_build_log, builder, build, print_logs=print_logs)
    build = builder.get_build(build.id)

    while build.status != "completed":
        await asyncio.sleep(5)
        build = builder.get_build(build.id)

    if build.push_completed_at:
        build_duration = build.build_completed_at - build.push_completed_at
        click.echo(f"Building completed in {build.image_name} {build_duration.seconds}")
    return build


async def _build_image(
    builder: ImageBuilderClient, image: Image, image_hash: str = "", print_logs=True
) -> Build:
    click.echo(f"Building {image._image_name}")
    fd, context_file = tempfile.mkstemp()
    image.build_context(context_file)

    click.echo(
        f"{image._image_name}: Posting {os.path.getsize(context_file)} bytes of context to build service...."
    )
    files = {"context": open(context_file, "rb")}
    data = {"name": image._image_name, "hash": image_hash}

    res = builder.client.post(
        f"{builder.build_service}/v1/builds",
        data=data,
        files=files,
        headers=builder.headers,
        timeout=60,
    )
    res.raise_for_status()
    build = Build.model_validate(res.json())
    return await _wait_for_build(builder, build, print_logs=print_logs)


def _show_failed_summary(builder: ImageBuilderClient, build: Build):
    click.secho(
        f"Building {build.image_name} failed with error message: {build.error_message}",
        fg="red",
    )

    log_response = builder.client.get(
        f"{builder.build_service}/v1/builds/{build.id}/log", headers=builder.headers
    )
    if log_response.status_code == 200:
        log = log_response.content.decode("utf-8")
        click.echo(log)
    elif log_response.status_code == 404:
        click.echo("Logs not found")
    else:
        log_response.raise_for_status()


async def _prepare_images(
    builder: ImageBuilderClient,
    images: Dict[Image, str],
    parallel_builds=False,
    retry=False,
):
    build_tasks = {}
    ready_builds: Dict[Image, Build] = {}

    # Iterate through the images and build anything that hasn't been built
    for image, image_hash in images.items():
        builds = builder.find_build(image._image_name, image_hash)

        if builds:
            build = builds[0]
            if build.status == "completed":
                if build.result == "failed":
                    _show_failed_summary(builder, build)
                    if retry:
                        click.secho(f"Retrying failed build '{build.image_name}'")
                        build = builder.retry_build(build.id)
                        task = asyncio.create_task(
                            _wait_for_build(
                                builder, build, print_logs=not parallel_builds
                            )
                        )
                        build_tasks[image] = task
                        if not parallel_builds:  # Await the task serially
                            await task

                else:
                    click.secho(f"Image '{build.image_name}' is built", fg="green")
                    ready_builds[image] = build

            elif build.status in ("ready", "building"):
                task = asyncio.create_task(
                    _wait_for_build(builder, build, print_logs=not parallel_builds)
                )
                build_tasks[image] = task
                if not parallel_builds:  # Await the task serially
                    await task

        else:
            task = asyncio.create_task(
                _build_image(
                    builder,
                    image,
                    image_hash=image_hash,
                    print_logs=not parallel_builds,
                )
            )
            build_tasks[image] = task
            if not parallel_builds:  # Await the task serially
                await task

    # Collect the results for our builds
    while True:
        for image, task in dict(build_tasks).items():
            if task.done():
                build_tasks.pop(image)
                build = task.result()
                if build.result != "failed":
                    ready_builds[image] = build

        if len(build_tasks) == 0:
            break
        else:
            await asyncio.sleep(1)

    # Find any blockers and report them to the users
    blockers = []
    for image in images:
        if image not in ready_builds:
            blockers.append(image)
            click.secho(
                f"Image {image._image_name} could not be built, this is blocking deployment",
                fg="red",
            )
        else:
            build = ready_builds[image]
            image.uri = build.uri

    if blockers:
        raise click.Abort


def _import_workflow_file(workflow):
    if "" not in sys.path:
        sys.path.insert(0, "")

    if workflow.endswith(".py"):
        workflow_path = pathlib.Path(workflow).resolve()
        sys.path.insert(0, str(workflow_path.parent))

        module_name = inspect.getmodulename(workflow)
        assert module_name is not None

        spec = importlib.util.spec_from_file_location(module_name, workflow_path)
        assert spec is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module

        assert spec.loader
        spec.loader.exec_module(module)

        return module
    else:
        raise click.ClickException("Workflow must be python files")


@click.command()
@click.argument("workflow_file", type=click.File("r"))
def prepare(workflow_file: click.File):
    """Prepare a workflow and it's artifacts for deployment."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder = ImageBuilderClient.from_env()
    seen_images: Dict[Image, str] = {}

    workflow = _import_workflow_file(workflow_file.name)
    for name in dir(workflow):
        obj = getattr(workflow, name)
        if isinstance(obj, Graph):
            click.echo(f"Found graph {name}")
            for node_name, node_obj in obj.nodes.items():
                image = node_obj.image
                if image is None:
                    raise click.ClickException(
                        f"graph function {node_name} needs to use an image"
                    )
                click.echo(
                    f"graph function {node_name} uses image '{image._image_name}'"
                )
                if image in seen_images:
                    continue
                seen_images[image] = image.hash()

    click.echo(f"Found {len(seen_images)} images in this workflow")
    asyncio.run(_prepare_images(builder, seen_images))


@click.command(help="Extract and display logs from tensorlake")
@click.option("--image", "-i")
def show_logs(image: str):
    if image:
        builder = ImageBuilderClient.from_env()
        if ":" in image:
            image, image_hash = image.split(":")
            build = builder.find_build(image, image_hash)[0]
        else:
            build = builder.get_latest_build(image)

        log_response = builder.client.get(
            f"{builder.build_service}/v1/builds/{build.id}/log", headers=builder.headers
        )
        if log_response.status_code == 200:
            log = log_response.content.decode("utf-8")
            print(log)


def _get_project_id():
    indexify_addr = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")
    introspect_response = httpx.post(
        f"{indexify_addr}/platform/v1/keys/introspect",
        headers={"Authorization": f"Bearer {os.getenv('TENSORLAKE_API_KEY')}"},
    )
    introspect_response.raise_for_status()
    return introspect_response.json()["projectId"]


@click.command(help="Return the project ID")
def get_project_id():
    click.echo(_get_project_id())


@click.command(help="Get URI for a given image")
@click.argument("image")
def get_image_uri(image: str):
    builder = ImageBuilderClient.from_env()
    build = builder.get_latest_build(image)
    print(build.uri)


tensorlake.add_command(get_image_uri)
tensorlake.add_command(get_project_id)
tensorlake.add_command(deploy)
tensorlake.add_command(prepare)
tensorlake.add_command(show_logs)

if __name__ == "__main__":
    tensorlake()
