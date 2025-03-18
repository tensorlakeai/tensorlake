import click

from tensorlake.builder.client import ImageBuilderClient


@click.command(help="Extract and display image building logs")
@click.option("--image", "-i", required=True, help="Image name")
def image_logs(image: str):
    builder = ImageBuilderClient.from_env()
    if ":" in image:
        image, image_hash = image.split(":")
        build = builder.find_build(image, image_hash)[0]
    else:
        build = builder.get_latest_build(image)

    if not build:
        click.echo(f"No builds found for image '{image}'")
        return

    log_response = builder.client.get(
        f"{builder.build_service}/v1/builds/{build.id}/log", headers=builder.headers
    )
    if log_response.status_code == 200:
        log = log_response.content.decode("utf-8")
        print(log)
    else:
        log_response.raise_for_status()
