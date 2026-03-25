from tensorlake.applications import Image

from images import BASE_IMAGE
from images.worker import WORKER_IMAGE

# A third image defined directly in this file.
API_IMAGE = Image(name="api-server", tag="stable", base_image="python:3.12-slim-bookworm").run(
    "pip install fastapi uvicorn"
)
