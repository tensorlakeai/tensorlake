import click

from tensorlake.builder.client import ImageBuilderClient


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


@click.command(help="Get URI for a given image")
@click.argument("image")
def get_image_uri(image: str):
    builder = ImageBuilderClient.from_env()
    build = builder.get_latest_build(image)
    click.echo(build.uri)
