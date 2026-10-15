"""The async binding must obey the same public contract as the sync binding."""

import unittest

from test_tunnel import TunnelTestsMixin

from tensorlake.sandbox import AsyncTcpTunnel


class TestAsyncTcpTunnel(TunnelTestsMixin, unittest.IsolatedAsyncioTestCase):
    tunnel_class = AsyncTcpTunnel


if __name__ == "__main__":
    unittest.main()
