"""Exercise credential isolation through the actual Python/Rust boundary."""

import concurrent.futures
import http.server
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from tensorlake import _cloud_sdk
except ImportError:
    _cloud_sdk = None


@unittest.skipIf(_cloud_sdk is None, "requires the built native SDK")
class TestDockerConfigNative(unittest.TestCase):
    def test_invalid_config_fails_before_build_or_import_for_both_backends(self):
        for cas in (False, True):
            for operation in ("build", "import"):
                with self.subTest(cas=cas, operation=operation):
                    args = dict(
                        api_url="http://127.0.0.1:1",
                        token="test-key",
                        cas=cas,
                        docker_config_json='{"auths":"do-not-log-this-secret"}',
                    )
                    with self.assertRaises(Exception) as raised:
                        if operation == "build":
                            _cloud_sdk.build_sandbox_image(
                                dockerfile_path="/nonexistent/Dockerfile", **args
                            )
                        else:
                            _cloud_sdk.import_sandbox_image(
                                image_reference="registry.example.com/app", **args
                            )
                    self.assertIn("Invalid Docker config JSON", str(raised.exception))
                    self.assertNotIn("do-not-log-this-secret", str(raised.exception))

    def test_concurrent_cas_builds_and_imports_forward_independent_credentials(self):
        captured = []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(
                    self.rfile.read(int(self.headers["Content-Length"]))
                )
                captured.append((self.path, payload))
                # Stop after request capture; no remote resources are provisioned.
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"message":"captured test request"}')

            def log_message(self, *_):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with (
                tempfile.TemporaryDirectory() as directory,
                patch.dict(
                    os.environ,
                    {"DOCKER_CONFIG": directory, "TENSORLAKE_IMAGE_SERVICE_URL": ""},
                ),
            ):
                Path(directory, "config.json").write_text("invalid ambient config")
                dockerfile = Path(directory, "Dockerfile")
                dockerfile.write_text("FROM scratch\n")

                def invoke(index):
                    raw = json.dumps(
                        {
                            "auths": {
                                "registry.example.com": {
                                    "identitytoken": f"test-token-{index}"
                                }
                            }
                        }
                    )
                    args = dict(
                        api_url=f"http://127.0.0.1:{server.server_port}",
                        token="test-key",
                        registered_name=f"test-{index}",
                        cas=True,
                        docker_config_json=raw,
                    )
                    with self.assertRaises(Exception) as raised:
                        if index % 2:
                            _cloud_sdk.build_sandbox_image(
                                dockerfile_path=str(dockerfile), **args
                            )
                        else:
                            _cloud_sdk.import_sandbox_image(
                                image_reference="registry.example.com/app:latest",
                                **args,
                            )
                    self.assertIn("captured test request", str(raised.exception))

                with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                    list(pool.map(invoke, range(4)))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

        self.assertEqual(len(captured), 4)
        for route, payload in captured:
            self.assertEqual(route, "/images/v4/builds")
            auth = json.loads(payload["registry_credentials_json"])["auths"]
            self.assertEqual(
                auth["registry.example.com"]["identitytoken"],
                payload["name"].replace("test-", "test-token-"),
            )
