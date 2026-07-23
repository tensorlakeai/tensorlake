import json
import unittest
from types import SimpleNamespace

from tensorlake.applications import application, function
from tensorlake.applications.remote.deploy import deploy_applications


@application(allow=["unauthenticated_requests"])
@function()
def public_endpoint_app(x: int) -> str:
    return str(x)


class FakeDeployClient:
    def __init__(self, existing_endpoint_id: str | None = None):
        self.existing_endpoint_id = existing_endpoint_id
        self.upserts: list[dict] = []

    def application(self, application_name: str):
        return SimpleNamespace(public_endpoint_id=self.existing_endpoint_id)

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        self.upserts.append(
            {
                "manifest": json.loads(manifest_json),
                "code_zip": code_zip,
                "upgrade_running_requests": upgrade_running_requests,
            }
        )


class TestDeployPublicEndpoint(unittest.TestCase):
    def _deployed_manifest(self, client: FakeDeployClient) -> dict:
        deploy_applications(__file__, api_client=client)
        return next(
            upsert["manifest"]
            for upsert in client.upserts
            if upsert["manifest"]["name"] == "public_endpoint_app"
        )

    def test_deploy_generates_public_endpoint_id(self):
        manifest = self._deployed_manifest(FakeDeployClient())

        self.assertEqual(manifest["allow"], ["unauthenticated_requests"])
        self.assertRegex(
            manifest["public_endpoint_id"],
            r"^endpoint_[A-Za-z0-9_-]{21}$",
        )

    def test_deploy_reuses_existing_public_endpoint_id(self):
        manifest = self._deployed_manifest(
            FakeDeployClient(existing_endpoint_id="endpoint_0123456789abcdefghijk")
        )

        self.assertEqual(
            manifest["public_endpoint_id"],
            "endpoint_0123456789abcdefghijk",
        )


if __name__ == "__main__":
    unittest.main()
