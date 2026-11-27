import unittest
from unittest.mock import patch

import tensorlake.cli._common as common_module
from tensorlake.cli._common import Context


class _FakeCloudClient:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def close(self):
        return None


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

    def test_rust_cloud_client_with_api_key_does_not_forward_org_project_scope(self):
        with patch.object(common_module, "CloudClient", _FakeCloudClient):
            context = Context.default(
                api_key="api-key",
                organization_id="org-1",
                project_id="proj-1",
            )
            client = context.rust_cloud_client

            self.assertIsInstance(client, _FakeCloudClient)
            self.assertEqual(client.kwargs["api_key"], "api-key")
            self.assertIsNone(client.kwargs["organization_id"])
            self.assertIsNone(client.kwargs["project_id"])

    def test_rust_cloud_client_prefers_api_key_over_pat_without_forwarding_scope(self):
        with patch.object(common_module, "CloudClient", _FakeCloudClient):
            context = Context.default(
                api_key="tl_apiKey_test",
                personal_access_token="tl_pat_test",
                organization_id="org-stale",
                project_id="project-stale",
            )
            client = context.rust_cloud_client

            self.assertEqual(client.kwargs["api_key"], "tl_apiKey_test")
            self.assertIsNone(client.kwargs["organization_id"])
            self.assertIsNone(client.kwargs["project_id"])

    def test_rust_cloud_client_requires_authentication(self):
        context = Context.default()
        with self.assertRaises(SystemExit):
            _ = context.rust_cloud_client


if __name__ == "__main__":
    unittest.main()
