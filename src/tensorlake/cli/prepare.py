import asyncio
from typing import Dict

import click

from src.tensorlake.cli.deploy import _import_workflow_file, _prepare_images
from tensorlake import Graph, Image
from tensorlake.builder.client import ImageBuilderClient


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
