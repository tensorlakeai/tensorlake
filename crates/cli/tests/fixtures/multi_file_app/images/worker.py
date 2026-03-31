from tensorlake.applications import Image

WORKER_IMAGE = Image(name="worker", base_image="python:3.12-slim-bookworm").run(
    "pip install celery redis"
)
