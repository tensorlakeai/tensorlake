import os
import tempfile
import time
from typing import Dict, List

import click

from tensorlake import Graph, Image, RemoteGraph, TensorlakeClient
from tensorlake.builder.client import ImageBuilderClient
from tensorlake.functions_sdk.image import Build


@click.group()
def tensorlake():
    pass


@click.command()
@click.argument("workflow_file", type=click.File("r"))
def deploy(workflow_file: click.File):
    """Deploy a workflow to tensorlake."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder = ImageBuilderClient.from_env()
    seen_images: Dict[Image, str] = {}
    deployed_graphs: List[Graph] = []

    # Read the graph file and build the images
    workflow_globals = {}
    with open(workflow_file.name, "r") as f:
        exec(f.read(), workflow_globals)

    for name, obj in workflow_globals.items():
        if isinstance(obj, Graph):
            deployed_graphs.append(obj)
            for node_name, node_obj in obj.nodes.items():
                image = node_obj.image
                if image in seen_images:
                    continue
                seen_images[image] = image.hash()

    _prepare_images(builder, seen_images)

    # If we are still here then our images should all have URIs

    # TODO: Fold calls to the platform API into a client class.
    indexify_addr = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")
    introspect_response = builder.client.post(
        f"{indexify_addr}/platform/v1/keys/introspect", headers=builder.headers
    )
    introspect_response.raise_for_status()
    project_id = introspect_response.json()["projectId"]

    client = TensorlakeClient(namespace=project_id, service_url="http://localhost:8900")
    click.secho("Everything looks good, deploying now", fg="green")
    for graph in deployed_graphs:
        # TODO: Every time we post we get a new version, is that expected or the client should do the checks?
        remote = RemoteGraph.deploy(graph, client=client)


def _wait_for_build(builder: ImageBuilderClient, build: Build):
    click.echo(f"Waiting for {build.image_name} to start building")
    while build.status != "building":
        time.sleep(1)
        build = builder.get_build(build.id)

    # Start streaming logs

    with builder.client.stream(
        "GET",
        f"{builder.build_service}/v1/builds/{build.id}/log",
        timeout=500,
        headers=builder.headers,
    ) as r:
        for line in r.iter_lines():
            print(line)

    if build.push_completed_at:
        build_duration = build.build_completed_at - build.push_completed_at
        click.echo(f"Building completed in {build.image_name} {build_duration.seconds}")
    return build


def _build_image(
    builder: ImageBuilderClient, image: Image, image_hash: str = ""
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

    return _wait_for_build(builder, build)


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


def _prepare_images(builder: ImageBuilderClient, images: Dict[Image, str]):
    ready_builds: Dict[Image, Build] = {}
    # Go through the images and build anything that hasn't been built
    for image, image_hash in images.items():
        builds = builder.find_build(image._image_name, image_hash)

        if builds:
            build = builds[0]
            if build.status == "completed":
                if build.result == "failed":
                    _show_failed_summary(builder, build)
                else:
                    click.secho(f"Image '{build.image_name}' is built", fg="green")
                    ready_builds[image] = build

            elif build.status in ("ready", "building"):
                build = _wait_for_build(builder, build)
                if build.result != "failed":
                    ready_builds[image] = build
                else:
                    _show_failed_summary(builder, build)

        else:
            ready_builds[image] = _build_image(builder, image, image_hash=image_hash)

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


@click.command()
@click.argument("workflow_file", type=click.File("r"))
def prepare(workflow_file: click.File):
    """Prepare a workflow and it's artifacts for deployment."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    client = ImageBuilderClient.from_env()
    seen_images: Dict[Image, str] = {}

    # Read the graph file and build the images
    workflow_globals = {}
    with open(workflow_file.name, "r") as f:
        exec(f.read(), workflow_globals)

    for name, obj in workflow_globals.items():
        if isinstance(obj, Graph):
            click.echo(f"Found graph {name}")
            for node_name, node_obj in obj.nodes.items():
                image = node_obj.image
                click.echo(
                    f"graph function {node_name} uses image '{image._image_name}'"
                )
                if image in seen_images:
                    continue
                seen_images[image] = image.hash()

    click.echo(f"Found {len(seen_images)} images in this workflow")
    _prepare_images(client, seen_images)


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


tensorlake.add_command(deploy)
tensorlake.add_command(prepare)
tensorlake.add_command(show_logs)

if __name__ == "__main__":
    tensorlake()
