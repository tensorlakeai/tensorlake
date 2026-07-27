import json
import os
import unittest
from typing import Any

import httpx

from tensorlake.applications import (
    HttpBody,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.api_client import APIClient
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.remote.manifests.application import (
    create_application_manifest,
)
from tensorlake.applications.remote.request import RemoteRequest


@application(allow=["unauthenticated_requests"])
@function()
def public_webhook(body: HttpBody) -> dict[str, Any]:
    headers = RequestContext.get().headers
    return {
        "body": body.text(),
        "content_type": body.content_type,
        "content_type_header": headers["content-type"],
        "test_headers": headers.getlist("x-tensorlake-test"),
        "has_cookie": "cookie" in headers,
    }


class TestPublicWebhook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)
        cls.api_url = os.environ["TENSORLAKE_API_URL"]
        cls.api_client = APIClient()
        cls.application_manifest = create_application_manifest(
            application_function=public_webhook,
            all_functions=get_functions(),
        )

    @classmethod
    def tearDownClass(cls):
        cls.api_client.close()

    def test_public_endpoint_is_generated_and_globally_resolvable(self):
        deployed_application = self.api_client.application("public_webhook")
        public_endpoint_id = deployed_application.public_endpoint_id

        self.assertIsInstance(public_endpoint_id, str)
        self.assertRegex(public_endpoint_id, r"^endpoint_[A-Za-z0-9_-]{21}$")

        response = httpx.get(
            f"{self.api_url}/internal/v1/applications/public/{public_endpoint_id}",
            timeout=30,
        )
        response.raise_for_status()
        self.assertEqual(
            response.json(),
            {
                "namespace": "default",
                "application_name": "public_webhook",
            },
        )

    def test_raw_body_and_sanitized_headers_reach_application(self):
        payload = {"event": "created", "id": 42}
        response = httpx.post(
            f"{self.api_url}/v1/namespaces/default/applications/public_webhook",
            content=json.dumps(payload).encode(),
            headers=[
                ("Content-Type", "application/json"),
                ("Accept", "application/json"),
                ("X-Tensorlake-Test", "first"),
                ("X-Tensorlake-Test", "second"),
                ("Cookie", "session=secret"),
            ],
            timeout=30,
        )
        response.raise_for_status()

        request = RemoteRequest(
            application_name="public_webhook",
            application_manifest=self.application_manifest,
            request_id=response.json()["request_id"],
            client=self.api_client,
        )
        output = request.output()

        self.assertEqual(output["body"], json.dumps(payload))
        self.assertEqual(output["content_type"], "application/json")
        self.assertEqual(output["content_type_header"], "application/json")
        self.assertEqual(output["test_headers"], ["first", "second"])
        self.assertFalse(output["has_cookie"])


if __name__ == "__main__":
    unittest.main()
