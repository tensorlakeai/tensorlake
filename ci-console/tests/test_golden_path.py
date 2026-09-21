from __future__ import annotations

import json
import threading
import time
import unittest
import urllib.error
import urllib.request

from backend.controller import RunnerController
from backend.runners import resolve_profile, runner_labels
from server import create_server


def workflow_job_payload(
    *,
    job_id: int = 123,
    action: str = "queued",
    labels: list[str] | None = None,
) -> dict:
    return {
        "action": action,
        "installation": {"id": 456},
        "organization": {"login": "acme"},
        "repository": {"full_name": "acme/api"},
        "workflow_job": {
            "id": job_id,
            "run_id": 789,
            "name": "test",
            "labels": labels or ["tensorlake"],
            "conclusion": "success" if action == "completed" else None,
        },
    }


class RunnerProfileTests(unittest.TestCase):
    def test_default_label_selects_safe_profile(self) -> None:
        profile = resolve_profile(["tensorlake"])
        self.assertIsNotNone(profile)
        self.assertEqual(profile.cpus, 2.0)
        self.assertEqual(profile.memory_mb, 8192)

    def test_size_label_controls_resources(self) -> None:
        profile = resolve_profile(["self-hosted", "tensorlake-8vcpu-ubuntu-2404"])
        self.assertIsNotNone(profile)
        self.assertEqual(profile.cpus, 8.0)
        self.assertEqual(profile.memory_mb, 32768)

    def test_unknown_tensorlake_label_does_not_provision(self) -> None:
        self.assertIsNone(resolve_profile(["tensorlake-1000vcpu"]))

    def test_jit_runner_matches_all_requested_labels(self) -> None:
        profile = resolve_profile(["tensorlake"])
        labels = runner_labels(["self-hosted", "tensorlake", "docker"], profile)
        self.assertEqual(
            labels,
            ["self-hosted", "linux", "x64", "tensorlake", "docker"],
        )


class ControllerTests(unittest.TestCase):
    def test_unrelated_jobs_are_ignored(self) -> None:
        controller = RunnerController(mode="demo", demo_delay=0.001)
        result = controller.handle_webhook(
            "workflow_job",
            "delivery-1",
            workflow_job_payload(labels=["ubuntu-latest"]),
        )
        self.assertEqual(result["action"], "ignored")
        self.assertEqual(controller.state.list_runs(), [])

    def test_label_is_the_complete_onboarding_contract(self) -> None:
        controller = RunnerController(mode="demo", demo_delay=0.001)
        result = controller.handle_webhook(
            "workflow_job", "delivery-1", workflow_job_payload()
        )
        self.assertEqual(result["action"], "provisioning")
        for _ in range(100):
            run = controller.state.get_run(result["run_id"])
            if run["status"] == "completed":
                break
            time.sleep(0.002)
        self.assertEqual(run["conclusion"], "success")
        self.assertEqual(run["label"], "tensorlake")
        self.assertEqual(run["sandbox"]["cpus"], 2.0)

    def test_duplicate_delivery_does_not_start_another_runner(self) -> None:
        controller = RunnerController(mode="demo", demo_delay=0.001)
        payload = workflow_job_payload()
        first = controller.handle_webhook("workflow_job", "same-id", payload)
        second = controller.handle_webhook("workflow_job", "same-id", payload)
        self.assertEqual(first["action"], "provisioning")
        self.assertEqual(second["action"], "duplicate")
        self.assertEqual(len(controller.state.list_runs()), 1)


class ServerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = create_server(port=0, mode="demo", demo_delay=0.001)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(
        self,
        path: str,
        *,
        method: str = "GET",
        body: dict | None = None,
    ) -> tuple[int, dict, dict]:
        data = json.dumps(body).encode() if body is not None else None
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request) as response:
            return response.status, json.load(response), dict(response.headers)

    def test_config_leads_with_one_runner_label(self) -> None:
        status, config, _ = self.request("/api/config")
        self.assertEqual(status, 200)
        self.assertEqual(config["default_label"], "tensorlake")
        self.assertEqual(config["install_url"], "/?installed=demo&account=acme")

    def test_http_demo_dispatches_labeled_job(self) -> None:
        status, result, _ = self.request(
            "/api/demo/jobs",
            method="POST",
            body={"label": "tensorlake"},
        )
        self.assertEqual(status, 202)
        for _ in range(100):
            _, run, _ = self.request(f"/api/runs/{result['run_id']}")
            if run["status"] == "completed":
                break
            time.sleep(0.002)
        self.assertEqual(run["conclusion"], "success")

    def test_old_migration_api_is_gone(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            self.request("/api/repositories")
        self.assertEqual(raised.exception.code, 404)

    def test_static_files_have_security_headers(self) -> None:
        request = urllib.request.Request(self.base_url + "/", method="HEAD")
        with urllib.request.urlopen(request) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["X-Frame-Options"], "DENY")
            self.assertIn(
                "frame-ancestors 'none'",
                response.headers["Content-Security-Policy"],
            )

    def test_backend_source_is_not_public(self) -> None:
        with self.assertRaises(urllib.error.HTTPError) as raised:
            urllib.request.urlopen(self.base_url + "/backend/github_app.py")
        self.assertEqual(raised.exception.code, 404)


if __name__ == "__main__":
    unittest.main()
