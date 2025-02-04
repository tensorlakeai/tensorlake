import os
import httpx
from tensorlake.documentai.common import DOC_AI_BASE_URL

class Jobs:
    
    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None, headers=self._headers())

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
        }
    
    def get(self, job_id: str) -> dict:
        response = self._client.get(
            url=f"jobs/{job_id}",
            headers=self._headers(),
        )
        response.raise_for_status()
        resp = response.json()
        return resp
    