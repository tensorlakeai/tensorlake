import unittest

from tensorlake.sandbox import Desktop, Sandbox, SandboxError


class _FakeRustDesktopClient:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.width = 1280
        self.height = 720
        self.calls = []

    def close(self):
        self.calls.append(("close",))

    def screenshot_png(self, timeout):
        self.calls.append(("screenshot_png", timeout))
        return b"\x89PNG\r\n"

    def move_mouse(self, x, y):
        self.calls.append(("move_mouse", x, y))

    def mouse_press(self, button, x, y):
        self.calls.append(("mouse_press", button, x, y))

    def mouse_release(self, button, x, y):
        self.calls.append(("mouse_release", button, x, y))

    def click(self, button, x, y):
        self.calls.append(("click", button, x, y))

    def double_click(self, button, x, y, delay_ms):
        self.calls.append(("double_click", button, x, y, delay_ms))

    def key_down(self, key):
        self.calls.append(("key_down", key))

    def key_up(self, key):
        self.calls.append(("key_up", key))

    def press(self, keys):
        self.calls.append(("press", list(keys)))

    def type_text(self, text):
        self.calls.append(("type_text", text))


class TestDesktopWrapper(unittest.TestCase):
    def test_connect_desktop_returns_desktop_wrapper(self):
        import tensorlake.sandbox.sandbox as sandbox_module

        previous = sandbox_module.RustCloudSandboxDesktopClient
        try:
            sandbox_module.RustCloudSandboxDesktopClient = _FakeRustDesktopClient
            sandbox = Sandbox(
                sandbox_id="sbx-1",
                proxy_url="https://sandbox.tensorlake.ai",
                api_key="k",
                organization_id="org-1",
                project_id="proj-1",
            )

            desktop = sandbox.connect_desktop(
                port=5902,
                password="secret",
                shared=False,
                connect_timeout=12.5,
            )

            self.assertIsInstance(desktop, Desktop)
            self.assertEqual(desktop.width, 1280)
            self.assertEqual(desktop.height, 720)
            self.assertEqual(desktop._rust_client.kwargs["proxy_url"], "https://sandbox.tensorlake.ai")
            self.assertEqual(desktop._rust_client.kwargs["sandbox_id"], "sbx-1")
            self.assertEqual(desktop._rust_client.kwargs["port"], 5902)
            self.assertEqual(desktop._rust_client.kwargs["password"], "secret")
            self.assertFalse(desktop._rust_client.kwargs["shared"])
            self.assertEqual(desktop._rust_client.kwargs["connect_timeout_sec"], 12.5)
            self.assertEqual(desktop._rust_client.kwargs["api_key"], "k")
            self.assertEqual(desktop._rust_client.kwargs["organization_id"], "org-1")
            self.assertEqual(desktop._rust_client.kwargs["project_id"], "proj-1")
        finally:
            sandbox_module.RustCloudSandboxDesktopClient = previous

    def test_desktop_methods_delegate_to_rust_client(self):
        rust_client = _FakeRustDesktopClient()
        desktop = Desktop(rust_client)

        screenshot = desktop.screenshot(timeout=2.0)
        desktop.move_mouse(10, 20)
        desktop.mouse_press("left", 10, 20)
        desktop.mouse_release("left")
        desktop.click(11, 12, button="middle")
        desktop.double_click(15, 16, button="right", delay_ms=75)
        desktop.left_click(1, 2)
        desktop.middle_click()
        desktop.right_click()
        desktop.key_down("enter")
        desktop.key_up("enter")
        desktop.press("a")
        desktop.press(["ctrl", "c"])
        desktop.type_text("hello")
        desktop.close()

        self.assertEqual(screenshot, b"\x89PNG\r\n")
        self.assertEqual(
            rust_client.calls,
            [
                ("screenshot_png", 2.0),
                ("move_mouse", 10, 20),
                ("mouse_press", "left", 10, 20),
                ("mouse_release", "left", None, None),
                ("click", "middle", 11, 12),
                ("double_click", "right", 15, 16, 75),
                ("click", "left", 1, 2),
                ("click", "middle", None, None),
                ("click", "right", None, None),
                ("key_down", "enter"),
                ("key_up", "enter"),
                ("press", ["a"]),
                ("press", ["ctrl", "c"]),
                ("type_text", "hello"),
                ("close",),
            ],
        )

    def test_desktop_maps_rust_errors_to_sandbox_error(self):
        class FakeRustError(Exception):
            pass

        class _FailingRustDesktopClient(_FakeRustDesktopClient):
            def click(self, button, x, y):
                raise FakeRustError(("internal", None, "desktop click failed"))

        import tensorlake.sandbox.desktop as desktop_module

        previous = desktop_module.RustCloudSandboxClientError
        try:
            desktop_module.RustCloudSandboxClientError = FakeRustError
            desktop = Desktop(_FailingRustDesktopClient())
            with self.assertRaisesRegex(SandboxError, "desktop click failed"):
                desktop.click()
        finally:
            desktop_module.RustCloudSandboxClientError = previous


if __name__ == "__main__":
    unittest.main()
