import unittest

from tensorlake.applications import application, function
from tensorlake.applications.remote.deploy import deploy_applications


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
    ) -> None:
        self.public_endpoint_upserts.append(
            {
                "application_name": application_name,
                "allow": allow,
            }
        )


class TestDeployPublicEndpoint(unittest.TestCase):
    def test_deploy_registers_public_endpoint_state(self):
        client = FakeDeployClient()

        deploy_applications(__file__, api_client=client)

        self.assertGreaterEqual(len(client.upserts), 1)
        self.assertIn(
            {
                "application_name": "public_endpoint_app",
                "allow": ["unauthorized_requests"],
            },
            client.public_endpoint_upserts,
        )


if __name__ == "__main__":
    unittest.main()
