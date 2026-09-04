from __future__ import annotations

import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from backend.controller import GoldenPathController
from backend.github_app import GitHubAppClient, GitHubError
from backend.runners import TensorlakeRunnerProvider
from server import create_server


class MigrationTests(unittest.TestCase):
    def test_migrates_supported_scalar_linux_labels(self) -> None:
        source = (
            "jobs:\n"
            "  test:\n"
            "    runs-on: ubuntu-latest\n"
            "  lint:\n"
            "    runs-on: 'ubuntu-22.04' # pinned\n"
        )
        migrated, changed = GitHubAppClient.migrate_workflow(source)
        self.assertEqual(changed, 2)
        self.assertIn("runs-on: tensorlake-2vcpu-ubuntu-2404", migrated)
        self.assertIn("runs-on: 'tensorlake-2vcpu-ubuntu-2204' # pinned", migrated)

    def test_does_not_rewrite_expressions_or_self_hosted_labels(self) -> None:
        source = (
            "jobs:\n"
            "  matrix:\n"
            "    runs-on: ${{ matrix.os }}\n"
            "  private:\n"
            "    runs-on: [self-hosted, linux]\n"
        )
        migrated, changed = GitHubAppClient.migrate_workflow(source)
        self.assertEqual(changed, 0)
        self.assertEqual(migrated, source)


class ControllerTests(unittest.TestCase):
    def test_requires_ordered_golden_path(self) -> None:
        controller = GoldenPathController(demo_delay=0.01)
        with self.assertRaises(GitHubError):
            controller.repositories()
        controller.connect()
        with self.assertRaises(ValueError):
            controller.create_pull_request()

    def test_demo_golden_path_completes(self) -> None:
        controller = GoldenPathController(demo_delay=0.01)
        connection = controller.connect()
        self.assertTrue(connection["connected"])
        repos = controller.repositories()
        plan = controller.plan([repos[0]["full_name"], repos[1]["full_name"]])
        self.assertEqual(plan["workflow_count"], 2)
        self.assertEqual(plan["replacement_count"], 2)
        pull_request = controller.create_pull_request()
        self.assertEqual(pull_request["status"], "open")
        run = controller.start_smoke_run()
        deadline = time.time() + 2
        while time.time() < deadline:
            run = controller.state.get_run(run["id"])
            if run and run["status"] == "completed":
                break
            time.sleep(0.01)
        self.assertIsNotNone(run)
        self.assertEqual(run["conclusion"], "success")
        self.assertGreaterEqual(len(run["logs"]), 7)
        self.assertTrue(run["sandbox"]["ephemeral"])
        self.assertTrue(run["ssh_command"].startswith("tl sbx ssh "))

    def test_waiting_run_is_claimed_by_matching_github_job(self) -> None:
        controller = GoldenPathController(demo_delay=0.01)
        waiting = controller.state.create_run("acme/api-gateway", "Smoke")
        claimed = controller.state.claim_waiting_run(
            "acme/api-gateway", 9876, 1234
        )
        self.assertEqual(claimed["id"], waiting["id"])
        self.assertEqual(claimed["github_job_id"], 9876)
        self.assertEqual(claimed["github_run_id"], 1234)

    def test_runner_label_controls_sandbox_resources(self) -> None:
        self.assertEqual(
            TensorlakeRunnerProvider._resources(
                ["self-hosted", "tensorlake-8vcpu-ubuntu-2404"]
            ),
            (8.0, 32768),
        )
        self.assertEqual(TensorlakeRunnerProvider._resources(["self-hosted"]), (2.0, 8192))


class ServerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = create_server(port=0, demo_delay=0.01)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()

    def request(self, method: str, path: str, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}",
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, json.load(response)

    def test_http_golden_path(self) -> None:
        status, health = self.request("GET", "/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["mode"], "demo")
        self.request("POST", "/api/github/connect", {})
        _, repositories = self.request("GET", "/api/repositories")
        selected = [repositories["repositories"][0]["full_name"]]
        status, plan = self.request(
            "POST", "/api/migration/plan", {"repositories": selected}
        )
        self.assertEqual(status, 200)
        self.assertIn("tensorlake-2vcpu", plan["changes"][0]["diff"])
        status, pull_request = self.request(
            "POST", "/api/migration/pull-request", {}
        )
        self.assertEqual(status, 201)
        self.assertEqual(pull_request["primary"]["number"], 42)
        status, run = self.request("POST", "/api/runs/smoke", {})
        self.assertEqual(status, 202)
        time.sleep(0.15)
        _, finished = self.request("GET", f"/api/runs/{run['id']}")
        self.assertEqual(finished["conclusion"], "success")
        _, reset = self.request("POST", "/api/session/reset", {})
        self.assertFalse(reset["connected"])
        self.assertIsNone(reset["latest_run"])

    def test_does_not_serve_backend_source(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.request("GET", "/backend/github_app.py")
        self.assertEqual(raised.exception.code, 404)

    def test_static_head_request_and_security_headers(self) -> None:
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/", method="HEAD"
        )
        with urllib.request.urlopen(request, timeout=3) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["X-Frame-Options"], "DENY")
            self.assertIn("default-src 'self'", response.headers["Content-Security-Policy"])


if __name__ == "__main__":
    unittest.main()
