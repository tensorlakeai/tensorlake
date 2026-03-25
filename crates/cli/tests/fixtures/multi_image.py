from tensorlake.applications import Image

ALPHA = Image(name="alpha", base_image="python:3.12-slim-bookworm").run(
    "pip install requests"
)

BETA = Image(name="beta", base_image="python:3.11-slim").run(
    "pip install numpy"
)
