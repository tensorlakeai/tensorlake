import unittest
from unittest.mock import patch, mock_open, MagicMock

import httpx

from tensorlake.utils.http_client import get_httpx_client


class TestHttpxClientCreation(unittest.TestCase):

    @patch("tensorlake.utils.http_client.ssl.create_default_context")
    def test_get_sync_client_no_tls(self, mock_ssl_context):
        mock_ssl_context.return_value = MagicMock()
        client = get_httpx_client()
        self.assertIsInstance(client, httpx.Client)

    @patch("tensorlake.utils.http_client.ssl.create_default_context")
    def test_get_async_client_no_tls(self, mock_ssl_context):
        mock_ssl_context.return_value = MagicMock()
        client = get_httpx_client(make_async=True)
        self.assertIsInstance(client, httpx.AsyncClient)

    @patch("builtins.open", new_callable=mock_open, read_data="yaml")
    @patch("tensorlake.utils.http_client.yaml.safe_load")
    @patch("tensorlake.utils.http_client.ssl.create_default_context")
    def test_get_sync_client_with_tls_config(
        self, mock_ssl_context, mock_yaml_load, mock_open
    ):
        mock_ctx = MagicMock()
        mock_ssl_context.return_value = mock_ctx
        mock_yaml_load.return_value = {
            "use_tls": True,
            "tls_config": {
                "cert_path": "path/to/cert.pem",
                "key_path": "path/to/key.pem",
                "ca_bundle_path": "path/to/ca.pem",
            },
        }

        client = get_httpx_client(config_path="dummy_config.yaml")
        mock_ssl_context.assert_called_once_with(cafile="path/to/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile="path/to/cert.pem", keyfile="path/to/key.pem"
        )
        self.assertIsInstance(client, httpx.Client)

    @patch("builtins.open", new_callable=mock_open, read_data="yaml")
    @patch("tensorlake.utils.http_client.yaml.safe_load")
    @patch("tensorlake.utils.http_client.ssl.create_default_context")
    def test_get_async_client_with_tls_config(
        self, mock_ssl_context, mock_yaml_load, mock_open
    ):
        mock_ctx = MagicMock()
        mock_ssl_context.return_value = mock_ctx
        mock_yaml_load.return_value = {
            "use_tls": True,
            "tls_config": {
                "cert_path": "path/to/cert.pem",
                "key_path": "path/to/key.pem",
                "ca_bundle_path": "path/to/ca.pem",
            },
        }

        client = get_httpx_client(config_path="dummy_config.yaml", make_async=True)
        mock_ssl_context.assert_called_once_with(cafile="path/to/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile="path/to/cert.pem", keyfile="path/to/key.pem"
        )
        self.assertIsInstance(client, httpx.AsyncClient)


if __name__ == "__main__":
    unittest.main()
