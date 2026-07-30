import http.server
import socket
import unittest
from unittest.mock import patch

from tensorlake.applications.request_context.http_server.server import (
    RequestContextHTTPServer,
)


class TestRequestContextHTTPServer(unittest.TestCase):
    def test_uses_ipv4_loopback_without_resolving_localhost(self):
        original_getaddrinfo = socket.getaddrinfo

        def reject_localhost(host, *args, **kwargs):
            self.assertNotEqual(host, "localhost")
            return original_getaddrinfo(host, *args, **kwargs)

        with patch("socket.getaddrinfo", side_effect=reject_localhost):
            server = RequestContextHTTPServer(http.server.BaseHTTPRequestHandler)

        try:
            self.assertEqual(server._httpd.server_address[0], "127.0.0.1")
            self.assertEqual(
                server.base_url,
                f"http://127.0.0.1:{server._httpd.server_address[1]}",
            )
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main()
