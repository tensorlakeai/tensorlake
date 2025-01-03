
from typing import Dict, Optional
import os
from indexify import Build, Image

import httpx

class ImageBuilderClient:
    def __init__(self, build_service: str):
        self.client = httpx
        self.build_service = build_service
        self.headers = {}
        api_key = os.getenv("TENSORLAKE_API_KEY")
        if api_key:
            self.headers["Authorization"] = f"Bearer {api_key}"

    def get(self, endpoint: str, params: Optional[Dict] = None):
        res = self.client.get(f"{self.build_service}{endpoint}", params=params, headers=self.headers)
        res.raise_for_status()
        return res.json()

    def get_build(self, build_id: int):
        return Build.model_validate(self.get(f"/builds/{build_id}"))
            
    def get_build_logs(self, build_id: int):
        log_response = self.client.get(f"{self.build_service}/builds/{build_id}/log")
        log_response.raise_for_status()
        return log_response.content.decode("utf-8")
    
    def post(self, endpoint: str, data: Dict, files: Dict):
        res = self.client.post(f"{self.build_service}{endpoint}", data=data, files=files, headers=self.headers, timeout=60)
        res.raise_for_status()
        return res.json()

