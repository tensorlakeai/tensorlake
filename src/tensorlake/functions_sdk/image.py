import datetime
import hashlib
import importlib
import logging
import os
import pathlib
import sys
import tarfile
from io import BytesIO
from typing import Dict, List, Optional

import docker
import docker.api.build
from pydantic import BaseModel


# Pydantic object for API
class ImageInformation(BaseModel):
    image_name: str
    image_hash: str
    image_uri: Optional[str] = ""
    sdk_version: str

    # These are deprecated and here for backwards compatibility
    run_strs: Optional[List[str]] = []
    tag: Optional[str] = ""
    base_image: Optional[str] = ""


HASH_BUFF_SIZE = 1024**2


class BuildOp(BaseModel):
    op_type: str
    options: Dict[str, str] = {}
    args: List[str]

    def hash(self, hash):
        if self.op_type in ("RUN", "ADD"):

            hash.update(self.op_type.encode())
            for a in self.args:
                hash.update(a.encode())

        elif self.op_type == "COPY":
            hash.update("COPY".encode())
            for root, dirs, files in os.walk(self.args[0]):
                for file in files:
                    filename = pathlib.Path(root, file)
                    with open(filename, "rb") as fp:
                        data = fp.read(HASH_BUFF_SIZE)
                        while data:
                            hash.update(data)
                            data = fp.read(HASH_BUFF_SIZE)

        else:
            raise ValueError(f"Unsupported build op type {self.op_type}")

    def render(self):
        if self.op_type in ("RUN", "ADD"):
            options = [f"--{k}={v}" for k, v in self.options.items()]
            return f"{self.op_type} {' '.join(options)} {' '.join(self.args)}"

        elif self.op_type == "COPY":
            return f"COPY {self.args[0]} {self.args[1]}"

        else:
            raise ValueError(f"Unsupported build op type {self.op_type}")


class Build(BaseModel):
    """
    Model for talking with the build service.
    """

    id: Optional[int] = None
    namespace: str
    image_name: str
    image_hash: str
    status: Optional[str] = None
    result: Optional[str] = None
    error_message: Optional[str] = None  # Only provided when result is "failed"

    created_at: Optional[datetime.datetime] = None
    started_at: Optional[datetime.datetime] = None
    build_completed_at: Optional[datetime.datetime] = None
    push_completed_at: Optional[datetime.datetime] = None
    uri: Optional[str] = None


class Image:
    def __init__(self, indexify_version: Optional[str] = None):
        self._image_name = None
        self._tag = "latest"
        self._base_image = BASE_IMAGE_NAME
        self._python_version = LOCAL_PYTHON_VERSION
        self._build_ops = []  # List of ImageOperation
        self._sdk_version = importlib.metadata.version("tensorlake")
        self._indexify_version = indexify_version
        self.uri = ""  # For internal use

    def name(self, image_name):
        self._image_name = image_name
        return self

    def tag(self, tag):
        self._tag = tag
        return self

    def base_image(self, base_image):
        self._base_image = base_image
        return self

    def add(self, source: str, dest: str, **kwargs):
        self._build_ops.append(
            BuildOp(op_type="ADD", args=[source, dest], options=kwargs)
        )
        return self

    def run(self, run_str, **kwargs):
        self._build_ops.append(BuildOp(op_type="RUN", args=[run_str], options=kwargs))
        return self

    def copy(self, source: str, dest: str, **kwargs):
        self._build_ops.append(
            BuildOp(op_type="COPY", args=[source, dest], options=kwargs)
        )
        return self

    def to_image_information(self):
        return ImageInformation(
            image_name=self._image_name,
            sdk_version=self._sdk_version,
            image_hash=self.hash(),
            image_uri=self.uri,
        )

    def build_context(self, filename: str):
        with tarfile.open(filename, "w:gz") as tf:
            for op in self._build_ops:
                if op.op_type == "COPY":
                    src = op.args[0]
                    logging.info(f"Adding {src}")
                    tf.add(src, src)

            dockerfile = self._generate_dockerfile()
            tarinfo = tarfile.TarInfo("Dockerfile")
            tarinfo.size = len(dockerfile)

            tf.addfile(tarinfo, BytesIO(dockerfile.encode()))

    def _generate_dockerfile(self, python_sdk_path: Optional[str] = None):
        docker_contents = [
            f"FROM {self._base_image}",
            "RUN mkdir -p ~/.indexify",
            f"RUN echo {self._image_name} > ~/.indexify/image_name",  # TODO: Do we still use this in executors?
            f"RUN echo {self.hash()} > ~/.indexify/image_hash",  # TODO: Do we still use this in executors?
            "WORKDIR /app",
        ]

        for build_op in self._build_ops:
            docker_contents.append(build_op.render())

        if python_sdk_path is not None:
            logging.info(
                f"Building image {self._image_name} with local version of the SDK"
            )
            if not os.path.exists(python_sdk_path):
                print(f"error: {python_sdk_path} does not exist")
                os.exit(1)
            docker_contents.append(f"COPY {python_sdk_path} /app/python-sdk")
            docker_contents.append("RUN (cd /app/python-sdk && pip install .)")
        else:
            # TODO: Remove installation of indexify when we've finished the container executor
            indexify_pkg = "indexify"
            if self._indexify_version is not None:
                indexfiy_pkg += f"=={self._indexify_version}"
            docker_contents.append(
                f"RUN pip install {indexify_pkg}"
            )  # TODO: Update this to specify our local tensorlake version

        docker_file = "\n".join(docker_contents)
        return docker_file

    def build(self, python_sdk_path: Optional[str] = None, docker_client=None):
        if docker_client is None:
            docker_client = docker.from_env()
            docker_client.ping()

        docker_file = self._generate_dockerfile(python_sdk_path=python_sdk_path)
        image_name = f"{self._image_name}:{self._tag}"

        docker.api.build.process_dockerfile = lambda dockerfile, path: (
            "Dockerfile",
            dockerfile,
        )

        return docker_client.images.build(
            path=".",
            dockerfile=docker_file,
            tag=image_name,
            rm=True,
        )

    def hash(self) -> str:
        hash = hashlib.sha256(
            self._image_name.encode()
        )  # Make a hash of the image name
        hash.update(self._base_image.encode())
        for op in self._build_ops:
            op.hash(hash)

        hash.update(self._sdk_version.encode())

        return hash.hexdigest()


LOCAL_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
BASE_IMAGE_NAME = f"python:{LOCAL_PYTHON_VERSION}-slim-bookworm"
