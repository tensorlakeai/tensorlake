import asyncio
from typing import Dict, Set

import click

from tensorlake import Graph, Image
from tensorlake.builder.client import ImageBuilderClient
from tensorlake.cli._common import AuthContext, with_auth
from tensorlake.cli.deploy import _import_workflow_file, _prepare_images
from tensorlake.cli.secrets import warning_missing_secrets


@click.command()
@click.argument("workflow_file", type=click.File("r"))
@with_auth
def prepare(auth: AuthContext, workflow_file: click.File):
    """Prepare a workflow and it's artifacts for deployment."""

    click.echo(f"Preparing deployment for {workflow_file.name}")
    builder = ImageBuilderClient.from_env()
    seen_images: Dict[Image, str] = {}

    workflow = _import_workflow_file(workflow_file.name)
    secret_names: Set[str] = set()
    for name in dir(workflow):
        obj = getattr(workflow, name)
        if isinstance(obj, Graph):
            click.echo(f"Found graph {name}")
            for node_name, node_obj in obj.nodes.items():
                [secret_names.add(secret) for secret in node_obj.secrets or []]
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

    warning_missing_secrets(auth, list(secret_names))
    click.echo(f"Found {len(seen_images)} images in this workflow")
    asyncio.run(_prepare_images(builder, seen_images))
