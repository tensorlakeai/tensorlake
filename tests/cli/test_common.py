import json
import unittest
from unittest.mock import patch

import tensorlake.cli._common as common_module
from tensorlake.cli._common import Context


class _FakeCloudClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def introspect_api_key_json(self):
        return json.dumps(
            {
                "id": "key-id",
                "projectId": "proj-1",
                "organizationId": "org-1",
            }
        )

    def list_secrets_json(self, organization_id, project_id, page_size):
        assert organization_id == "org-1"
        assert project_id == "proj-1"
        assert page_size == 100
        return json.dumps(
            {
                "items": [
                    {"name": "SECRET_A"},
                    {"name": "SECRET_B"},
                ]
            }
        )

    def close(self):
        return None


class _FailIntrospectCloudClient(_FakeCloudClient):
    def introspect_api_key_json(self):
        raise RuntimeError("introspection unavailable")


class TestContext(unittest.TestCase):
    def test_default_resolves_cloud_url_from_api_url(self):
        context = Context.default(api_url="https://api.tensorlake.dev", api_key="key")
        self.assertEqual(context.cloud_url, "https://cloud.tensorlake.dev")

    def test_default_falls_back_to_public_cloud_url_for_custom_api_url(self):
        context = Context.default(api_url="http://localhost:8900", api_key="key")
        self.assertEqual(context.cloud_url, "https://cloud.tensorlake.ai")

    def test_rust_cloud_client_uses_api_key(self):
        with patch.object(common_module, "CloudClient", _FakeCloudClient):
            context = Context.default(api_key="api-key")
            client = context.rust_cloud_client

            self.assertIsInstance(client, _FakeCloudClient)
            self.assertEqual(client.kwargs["api_key"], "api-key")
            self.assertEqual(client.kwargs["api_url"], "https://api.tensorlake.ai")
            self.assertEqual(client.kwargs["namespace"], "default")

    def test_rust_cloud_client_uses_pat(self):
        with patch.object(common_module, "CloudClient", _FakeCloudClient):
            context = Context.default(
                personal_access_token="pat-token",
                organization_id="org-1",
                project_id="proj-1",
            )
            client = context.rust_cloud_client

            self.assertIsInstance(client, _FakeCloudClient)
            self.assertEqual(client.kwargs["api_key"], "pat-token")
            self.assertEqual(client.kwargs["organization_id"], "org-1")
            self.assertEqual(client.kwargs["project_id"], "proj-1")

    def test_rust_cloud_client_requires_authentication(self):
        context = Context.default()
        with self.assertRaises(SystemExit):
            _ = context.rust_cloud_client

    def test_list_secret_names_uses_rust_client(self):
        with patch.object(common_module, "CloudClient", _FakeCloudClient):
            context = Context.default(api_key="api-key")
            secret_names = context.list_secret_names(page_size=100)
            self.assertEqual(secret_names, ["SECRET_A", "SECRET_B"])

    def test_list_secret_names_uses_provided_org_project_without_introspection(self):
        with patch.object(common_module, "CloudClient", _FailIntrospectCloudClient):
            context = Context.default(
                api_key="api-key",
                organization_id="org-1",
                project_id="proj-1",
            )
            secret_names = context.list_secret_names(page_size=100)

            self.assertEqual(secret_names, ["SECRET_A", "SECRET_B"])


if __name__ == "__main__":
    unittest.main()
