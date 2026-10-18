"""Real TLS regressions: mocks cannot expose concurrent SSL socket failures."""

import asyncio
import shutil
import ssl
import subprocess
import tempfile
import unittest
from pathlib import Path

from websockets.asyncio.server import serve

from tensorlake.sandbox import TcpTunnel


@unittest.skipUnless(
    shutil.which("openssl"), "requires openssl to create a test certificate"
)
class TestTunnelTLS(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.files = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.files.cleanup)
        cert = Path(cls.files.name) / "cert.pem"
        key = Path(cls.files.name) / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=IP:127.0.0.1,DNS:localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        cls.server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        cls.server_tls.load_cert_chain(cert, key)

    async def test_untrusted_tls_certificate_is_rejected(self):
        loop = asyncio.get_running_loop()
        original_handler = loop.get_exception_handler()
        tls_alerts = []

        def handle_exception(loop, context):
            error = context.get("exception")
            if isinstance(error, ssl.SSLError):
                tls_alerts.append(error)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(handle_exception)
        self.addCleanup(loop.set_exception_handler, original_handler)

        async def peer(ws):
            self.fail("untrusted TLS must not reach the websocket handshake")

        async with serve(peer, "127.0.0.1", 0, ssl=self.server_tls) as server:
            port = server.sockets[0].getsockname()[1]
            with TcpTunnel.listen(
                base_url=f"https://127.0.0.1:{port}",
                ws_headers={},
                remote_port=9000,
                local_port=0,
            ) as tunnel:
                reader, writer = await asyncio.open_connection(
                    tunnel.local_host, tunnel.local_port
                )
                try:
                    self.assertEqual(await asyncio.wait_for(reader.read(), 3), b"")
                finally:
                    writer.close()
                    await writer.wait_closed()
            self.assertTrue(tls_alerts)

    async def test_sync_tunnel_can_be_used_inside_a_running_event_loop(self):
        # Construction/close do not call asyncio.run on the caller's thread.
        with TcpTunnel.listen(
            base_url="https://localhost", ws_headers={}, remote_port=9000, local_port=0
        ) as tunnel:
            self.assertFalse(tunnel.closed)
        self.assertTrue(tunnel.closed)


if __name__ == "__main__":
    unittest.main()
