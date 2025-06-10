import asyncio
import os
import tempfile
from typing import Dict, List

import click

from tensorlake import Graph, Image
from tensorlake.builder.client import ImageBuilderClient
from tensorlake.builder.client_v2 import ImageBuilderV2Client
from tensorlake.cli._common import AuthContext, pass_auth
from tensorlake.cli.secrets import warning_missing_secrets
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.functions_sdk.image import Build
from tensorlake.functions_sdk.workflow_module import (
    ImageInfo,
    WorkflowModuleInfo,
    load_workflow_module_info,
)
from tensorlake.remote_graph import RemoteGraph


@click.command()
@click.option("-p", "--parallel-builds", is_flag=True, default=False)
@click.option("-r", "--retry", is_flag=True, default=False)
@click.option("--upgrade-queued-requests", is_flag=True, default=False)
@click.option("--builder-v2", is_flag=True, default=False)
@click.argument("workflow_file", type=click.File("r"))
@pass_auth
def deploy(
    auth: AuthContext,
    workflow_file: click.File,
    parallel_builds: bool,
    retry: bool,
    upgrade_queued_requests: bool,
    builder_v2: bool,
):
    """Deploy a workflow to tensorlake."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder = ImageBuilderClient.from_env()
    builder_v2 = ImageBuilderV2Client.from_env() if builder_v2 else None

    try:
        workflow_module_info: WorkflowModuleInfo = load_workflow_module_info(
            workflow_file.name
        )
    except Exception as e:
        click.secho(
            f"Failed loading workflow file, please check the error message: {e}",
            fg="red",
        )
        raise click.Abort

    _validate_workflow_module(workflow_module_info, auth)

    if builder_v2:
        asyncio.run(_prepare_images_v2(builder_v2, workflow_module_info.images))
    else:
        asyncio.run(
            _prepare_images(
                builder,
                list(workflow_module_info.images.keys()),
                parallel_builds=parallel_builds,
                retry=retry,
            )
        )

    click.secho("Everything looks good, deploying now", fg="green")
    _deploy_graphs(
        graphs=workflow_module_info.graphs,
        code_dir_path=graph_code_dir_path(workflow_file.name),
        upgrade_queued_requests=upgrade_queued_requests,
    )


def _validate_workflow_module(
    workflow_module_info: WorkflowModuleInfo, auth: AuthContext
):
    # Validate the workflow module contents for compatibility with Tensorlake Cloud requirements.
    for graph in workflow_module_info.graphs:
        for node in graph.nodes.values():
            if node.image is None:
                raise click.ClickException(
                    f"graph {graph.name} function {node.name} needs to use an image"
                )
    if len(workflow_module_info.graphs) == 0:
        raise click.UsageError(
            "No graphs found in the workflow file, make sure at least one graph is defined as a global variable."
        )
    warning_missing_secrets(auth, list(workflow_module_info.secret_names))


def _stream_build_log(builder: ImageBuilderClient, build: Build):
    with builder.client.stream(
        "GET",
        f"{builder.build_service}/v1/builds/{build.id}/log",
        timeout=600,
        headers=builder.headers,
    ) as r:
        for line in r.iter_lines():
            print(line)


async def _wait_for_build(builder: ImageBuilderClient, build: Build, print_logs=True):
    click.echo(f"Waiting for {build.image_name} to start building")
    while build.status == "ready":
        await asyncio.sleep(5)
        build = builder.get_build(build.id)

    # Start streaming logs
    if print_logs:
        await asyncio.to_thread(_stream_build_log, builder, build)
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


async def _prepare_images_v2(
    builder_v2: ImageBuilderV2Client, images: Dict[Image, ImageInfo]
):
    for image_info in images.values():
        for build_context in image_info.build_contexts:

            await builder_v2.build(build_context, image_info.image)

    click.secho(f"Built {len(images)} images with builder v2", fg="green")


async def _prepare_images(
    builder: ImageBuilderClient,
    images: List[Image],
    parallel_builds=False,
    retry=False,
):
    build_tasks: Dict[asyncio.Task[Build], Image] = {}
    ready_builds: Dict[Image, Build] = {}

    # Iterate through the images and build anything that hasn't been built
    for image in images:
        image_hash = image.hash()
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
                        build_tasks[task] = image
                        if not parallel_builds:  # Await the task serially
                            await task

                else:
                    click.secho(f"Image '{build.image_name}' is built", fg="green")
                    ready_builds[image] = build

            elif build.status in ("ready", "building"):
                task = asyncio.create_task(
                    _wait_for_build(builder, build, print_logs=not parallel_builds)
                )
                build_tasks[task] = image
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
            build_tasks[task] = image
            if not parallel_builds:  # Await the task serially
                await task

    # Collect the results for our builds
    while len(build_tasks):
        for task, image in dict(build_tasks).items():
            if task.done():
                build_tasks.pop(task)
                if task_exc := task.exception():
                    raise task_exc
                build = task.result()
                if build.result != "failed":
                    ready_builds[image] = build

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


def _deploy_graphs(
    graphs: List[Graph], code_dir_path: str, upgrade_queued_requests: bool
):
    for graph in graphs:
        try:
            RemoteGraph.deploy(
                graph,
                code_dir_path=code_dir_path,
                upgrade_tasks_to_latest_version=upgrade_queued_requests,
            )
        except Exception as e:
            click.secho(
                f"Graph {graph.name} could not be deployed, please check the error message: {e}",
                fg="red",
            )
            raise click.Abort

        click.secho(f"Deployed {graph.name}", fg="green")
