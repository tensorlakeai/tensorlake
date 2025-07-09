import asyncio
from typing import Dict, List

import click

from tensorlake import Graph, Image
from tensorlake.builder.client_v2 import ImageBuilderV2Client
from tensorlake.cli._common import AuthContext, pass_auth
from tensorlake.cli.secrets import warning_missing_secrets
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
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
    # TODO: implement with image builder v2
    parallel_builds: bool,
    # Keeping --retry flag for backward compatibility with stable SDK.
    # TODO: remove the retry option once we stop using the stable SDK version.
    retry: bool,
    upgrade_queued_requests: bool,
    # We use builder v2 unconditionally, keeping --builder-v2 flag for backward compatibility with
    # stable SDK. TODO: remove the option once we stop using the stable SDK version.
    builder_v2: bool,
):
    """Deploy a workflow to tensorlake."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder_v2 = ImageBuilderV2Client.from_env()

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

    asyncio.run(_prepare_images_v2(builder_v2, workflow_module_info.images))

    click.secho("Everything looks good, deploying now", fg="green")
    _deploy_graphs(
        graphs=workflow_module_info.graphs,
        code_dir_path=graph_code_dir_path(workflow_file.name),
        upgrade_queued_requests=upgrade_queued_requests,
    )


def _validate_workflow_module(
    workflow_module_info: WorkflowModuleInfo, auth: AuthContext
):
    if len(workflow_module_info.graphs) == 0:
        raise click.UsageError(
            "No graphs found in the workflow file, make sure at least one graph is defined as a global variable."
        )
    warning_missing_secrets(auth, list(workflow_module_info.secret_names))


async def _prepare_images_v2(
    builder_v2: ImageBuilderV2Client, images: Dict[Image, ImageInfo]
):
    for image_info in images.values():
        for build_context in image_info.build_contexts:
            await builder_v2.build(build_context, image_info.image)

    click.secho(f"Built {len(images)} images with builder v2", fg="green")


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
