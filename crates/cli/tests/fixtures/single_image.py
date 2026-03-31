from tensorlake.applications import Image

MY_IMAGE = (
    Image(name="my-app", tag="v1", base_image="python:3.12-slim-bookworm")
    .run("pip install requests")
    .env("APP_ENV", "production")
)
