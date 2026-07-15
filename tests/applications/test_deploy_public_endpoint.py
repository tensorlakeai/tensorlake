import unittest

from tensorlake.applications import application, function
from tensorlake.applications.remote.api_client import (
    ApplicationPublicEndpoint,
    ApplicationPublicEndpoints,
)
from tensorlake.applications.remote.deploy import (
    _enabled_public_endpoint_url,
    deploy_applications,
)


@application(allow=["unauthorized_requests"])
@function()
def public_endpoint_app(x: int) -> str:
    return str(x)


class FakeDeployClient:
    def __init__(self):
        self.upserts: list[dict] = []
        self.public_endpoint_upserts: list[dict] = []

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        self.upserts.append(
            {
                "manifest_json": manifest_json,
                "code_zip": code_zip,
                "upgrade_running_requests": upgrade_running_requests,
            }
        )

    def ensure_application_public_endpoint(
        self,
        application_name: str,
        allow: list[str],
    ) -> ApplicationPublicEndpoints:
        self.public_endpoint_upserts.append(
            {
                "application_name": application_name,
                "allow": allow,
            }
        )
        return ApplicationPublicEndpoints(
            application_name=application_name,
            allow_unauthorized_requests="unauthorized_requests" in allow,
            endpoints=[
                ApplicationPublicEndpoint(
                    id="endpoint_123",
                    url="https://api.tensorlake.ai/applications/public/endpoint_123",
                    enabled=True,
                    created_at="2026-07-10T00:00:00Z",
                    updated_at="2026-07-10T00:00:00Z",
                )
            ],
        )


class TestDeployPublicEndpoint(unittest.TestCase):
    def test_extracts_enabled_endpoint_from_cloud_client_json(self):
        response = """{
            "application_name": "public_endpoint_app",
            "allow_unauthorized_requests": true,
            "endpoints": [{
                "id": "endpoint_123",
                "url": "https://api.tensorlake.ai/applications/public/endpoint_123",
                "enabled": true,
                "created_at": "2026-07-10T00:00:00Z",
                "updated_at": "2026-07-10T00:00:00Z"
            }]
        }"""

        self.assertEqual(
            _enabled_public_endpoint_url(response),
            "https://api.tensorlake.ai/applications/public/endpoint_123",
        )

    def test_ignores_disabled_endpoint(self):
        self.assertIsNone(
            _enabled_public_endpoint_url(
                {
                    "allow_unauthorized_requests": False,
                    "endpoints": [
                        {
                            "url": "https://api.tensorlake.ai/applications/public/endpoint_123",
                            "enabled": False,
                        }
                    ],
                }
            )
        )

    def test_deploy_registers_public_endpoint_state(self):
        client = FakeDeployClient()

        public_endpoint_urls = deploy_applications(__file__, api_client=client)

        self.assertGreaterEqual(len(client.upserts), 1)
        self.assertIn(
            {
                "application_name": "public_endpoint_app",
                "allow": ["unauthorized_requests"],
            },
            client.public_endpoint_upserts,
        )
        self.assertEqual(
            public_endpoint_urls,
            {
                "public_endpoint_app": "https://api.tensorlake.ai/applications/public/endpoint_123"
            },
        )


if __name__ == "__main__":
    unittest.main()
