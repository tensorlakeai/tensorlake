import unittest

import httpx

from tensorlake.cli._common import Context, raise_on_authn_authz


class TestContext(unittest.TestCase):
    def test_default_resolves_cloud_url_from_api_url(self):
        context = Context.default(api_url="https://api.tensorlake.dev", api_key="key")
        self.assertEqual(context.cloud_url, "https://cloud.tensorlake.dev")

    def test_default_falls_back_to_public_cloud_url_for_custom_api_url(self):
        context = Context.default(api_url="http://localhost:8900", api_key="key")
        self.assertEqual(context.cloud_url, "https://cloud.tensorlake.ai")

    def test_client_uses_api_key_auth_header(self):
        context = Context.default(api_key="api-key")
        client = context.client
        self.addCleanup(client.close)

        self.assertEqual(client.headers.get("Authorization"), "Bearer api-key")
        self.assertIn("Tensorlake CLI", client.headers.get("User-Agent", ""))

    def test_client_uses_pat_auth_and_forwarded_headers(self):
        context = Context.default(
            personal_access_token="pat-token",
            organization_id="org-1",
            project_id="proj-1",
        )
        client = context.client
        self.addCleanup(client.close)

        self.assertEqual(client.headers.get("Authorization"), "Bearer pat-token")
        self.assertEqual(client.headers.get("X-Forwarded-Organization-Id"), "org-1")
        self.assertEqual(client.headers.get("X-Forwarded-Project-Id"), "proj-1")

    def test_client_requires_authentication(self):
        context = Context.default()
        with self.assertRaises(SystemExit):
            _ = context.client


class TestAuthHooks(unittest.TestCase):
    def test_raise_on_authn_authz_allows_non_auth_errors(self):
        response = httpx.Response(
            200, request=httpx.Request("GET", "https://api.tensorlake.ai/ping")
        )
        raise_on_authn_authz(response)

    def test_raise_on_authn_authz_exits_on_401(self):
        response = httpx.Response(
            401, request=httpx.Request("GET", "https://api.tensorlake.ai/ping")
        )
        with self.assertRaises(SystemExit):
            raise_on_authn_authz(response)

    def test_raise_on_authn_authz_exits_on_403(self):
        response = httpx.Response(
            403, request=httpx.Request("GET", "https://api.tensorlake.ai/ping")
        )
        with self.assertRaises(SystemExit):
            raise_on_authn_authz(response)


if __name__ == "__main__":
    unittest.main()
