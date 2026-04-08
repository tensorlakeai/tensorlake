import json
import queue
import types
import unittest
from unittest.mock import patch

from tensorlake.sandbox import Pty, Sandbox


class _FakeRustProxyClient:
    def __init__(self, *args, **kwargs):
        self.start_payload_json = None

    def close(self):
        return None

    def base_url(self):
        return "http://localhost:9443"

    def create_pty_session_json(self, payload_json):
        self.start_payload_json = payload_json
        return json.dumps({"session_id": "sess-1", "token": "tok-1"})


class _FakeWebSocketConnection:
    def __init__(self):
        self.sent = []
        self.frames = queue.Queue()
        self.close_status_code = 1000
        self.close_reason = ""

    def send_binary(self, data):
        self.sent.append(bytes(data))

    def recv(self):
        item = self.frames.get(timeout=1)
        if isinstance(item, Exception):
            raise item
        return item

    def close(self, status=1000, reason="client disconnect"):
        self.close_status_code = status
        self.close_reason = reason
        self.frames.put(None)


class TestPty(unittest.TestCase):
    def make_sandbox(self):
        import tensorlake.sandbox.sandbox as sandbox_module

        fake = _FakeRustProxyClient()
        with patch.object(
            sandbox_module, "_RUST_SANDBOX_PROXY_CLIENT_AVAILABLE", True
        ), patch.object(
            sandbox_module,
            "RustCloudSandboxProxyClient",
            side_effect=lambda **kwargs: fake,
        ):
            sandbox = Sandbox(
                sandbox_id="sbx-1",
                proxy_url="http://localhost:9443",
                api_key="secret",
            )
        sandbox._rust_client = fake
        sandbox._base_url = fake.base_url()
        return sandbox, fake

    def test_create_pty_returns_connected_handle(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, fake_rust = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        fake_websocket_module = types.SimpleNamespace(
            create_connection=lambda *args, **kwargs: fake_ws
        )
        seen_data = []
        seen_exit = []

        with patch.object(pty_module, "websocket", fake_websocket_module):
            pty = sandbox.create_pty(
                command="/bin/bash",
                on_data=seen_data.append,
                on_exit=seen_exit.append,
            )

            self.assertIsInstance(pty, Pty)
            self.assertIn("token=tok-1", pty._ws_url)
            self.assertEqual(pty._ws_headers["X-PTY-Token"], "tok-1")
            self.assertEqual(fake_ws.sent[0], b"\x02")

            pty.send_input("pwd\n")
            pty.resize(120, 40)
            self.assertEqual(fake_ws.sent[1], b"\x00pwd\n")
            self.assertEqual(fake_ws.sent[2], b"\x01\x00x\x00(")

            fake_ws.frames.put(b"\x00hi")
            fake_ws.frames.put(b"\x03\x00\x00\x00\x07")

            self.assertEqual(pty.wait(timeout=1), 7)
            self.assertEqual(seen_data, [b"hi"])
            self.assertEqual(seen_exit, [7])

        payload = json.loads(fake_rust.start_payload_json)
        self.assertEqual(payload["command"], "/bin/bash")

    def test_disconnect_and_reconnect(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = self.make_sandbox()
        first = _FakeWebSocketConnection()
        second = _FakeWebSocketConnection()
        sockets = queue.Queue()
        sockets.put(first)
        sockets.put(second)
        fake_websocket_module = types.SimpleNamespace(
            create_connection=lambda *args, **kwargs: sockets.get_nowait()
        )

        with patch.object(pty_module, "websocket", fake_websocket_module):
            pty = sandbox.create_pty(command="/bin/bash")
            pty.disconnect()
            for _ in range(100):
                if not pty.connected:
                    break
            pty.connect()

            second.frames.put(None)
            second.close_reason = "exit:0"
            self.assertEqual(pty.wait(timeout=1), 0)
            self.assertEqual(first.sent[0], b"\x02")
            self.assertEqual(second.sent[0], b"\x02")

    def test_kill_pty_uses_http_api(self):
        import tensorlake.sandbox.pty as pty_module

        sandbox, _ = self.make_sandbox()
        fake_ws = _FakeWebSocketConnection()
        fake_websocket_module = types.SimpleNamespace(
            create_connection=lambda *args, **kwargs: fake_ws
        )

        with patch.object(pty_module, "websocket", fake_websocket_module):
            pty = sandbox.create_pty(command="/bin/bash")

            with patch.object(pty_module.httpx, "delete") as delete_mock:
                delete_mock.return_value = types.SimpleNamespace(
                    is_success=True,
                    status_code=204,
                    text="",
                )
                pty.kill()

                delete_mock.assert_called_once()
                self.assertIn("/api/v1/pty/sess-1", delete_mock.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
