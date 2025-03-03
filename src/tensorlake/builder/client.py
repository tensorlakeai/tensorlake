import os
from typing import Dict, Optional

import httpx

from tensorlake.functions_sdk.image import Build


class ImageBuilderClient:
    def __init__(self, build_service: str, api_key):
        self.client = httpx
        self.build_service = build_service
        self.headers = {}
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

    @classmethod
    def from_env(cls):
        api_key = os.getenv("TENSORLAKE_API_KEY")
        indexify_url = os.getenv("INDEXIFY_URL", "https://api.tensorlake.ai")
        build_url = os.getenv(
            "TENSORLAKE_BUILD_SERVICE", f"{indexify_url}/images"
        )  # Mainly used for debugging/local testing
        return cls(build_url, api_key)

    def get(self, endpoint: str, params: Optional[Dict] = None):
        res = self.client.get(
            f"{self.build_service}{endpoint}", params=params, headers=self.headers
        )
        res.raise_for_status()
        return res.json()

    def get_build(self, build_id: int):
        return Build.model_validate(self.get(f"/v1/builds/{build_id}"))

    def get_build_logs(self, build_id: int):
        log_response = self.client.get(f"{self.build_service}/builds/{build_id}/log")
        log_response.raise_for_status()
        return log_response.content.decode("utf-8")

    def post(self, endpoint: str, data: Dict, files: Dict):
        res = self.client.post(
            f"{self.build_service}{endpoint}",
            data=data,
            files=files,
            headers=self.headers,
            timeout=60,
        )
        res.raise_for_status()
        return res.json()

    def build_exists(self, image_name: str, image_hash: str):
        builds = self.find_build(image_name, image_hash)
        return builds != []

    def find_build(self, image_name: str, image_hash: str):
        params = {"image_name": image_name, "image_hash": image_hash}
        res = self.client.get(
            f"{self.build_service}/v1/builds", headers=self.headers, params=params
        )
        res.raise_for_status()
        result = [Build.model_validate(b) for b in res.json()]
        result.sort(key=lambda b: b.build_completed_at, reverse=True)
        return result

    def get_latest_build(self, image_name: str) -> Build:
        res = self.client.get(
            f"{self.build_service}/v1/builds",
            headers=self.headers,
            params={"image_name": image_name},
        )
        res.raise_for_status()
        builds = [Build.model_validate(b) for b in res.json()]
        builds.sort(key=lambda b: b.created_at, reverse=True)
        if builds:
            return builds[0]

    def retry_build(self, build_id: int):
        request = self.client.post(
            f"{self.build_service}/v1/builds/{build_id}/rebuild",
            headers=self.headers,
        )
        request.raise_for_status()

        return Build.model_validate(request.json())
