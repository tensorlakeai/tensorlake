from tensorlake.applications import Image

BASE_IMAGE = Image(name="imported-base", base_image="python:3.12-slim-bookworm").run(
    "pip install httpx"
)
