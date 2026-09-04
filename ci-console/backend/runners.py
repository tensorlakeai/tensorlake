from __future__ import annotations

import os
import threading
import time
from typing import Any

from .github_app import GitHubAppClient
from .state import GoldenPathState


class DemoRunnerProvider:
    def __init__(self, state: GoldenPathState, delay: float = 0.35):
        self.state = state
        self.delay = delay

    def start(self, repository: str, workflow: str) -> dict[str, Any]:
        run = self.state.create_run(repository, workflow)
        threading.Thread(
            target=self._execute, args=(run["id"],), daemon=True
        ).start()
        return run

    def _pause(self) -> None:
        time.sleep(self.delay)

    def _execute(self, run_id: str) -> None:
        self.state.append_log(run_id, "Waiting for a matching GitHub Actions job...")
        self._pause()
        self.state.set_step(run_id, "queued", "complete")
        self.state.set_step(run_id, "sandbox", "active")
        self.state.update_run(run_id, status="provisioning")
        self.state.append_log(run_id, "Creating isolated Firecracker microVM")
        self._pause()
        sandbox_id = "sbx_7f9a2c18"
        self.state.update_run(
            run_id,
            sandbox={
                "id": sandbox_id,
                "status": "running",
                "image": "tensorlake/github-runner:ubuntu-24.04",
                "cpus": 2,
                "memory_mb": 8192,
                "ephemeral": True,
            },
            ssh_command=f"tl sbx ssh {sandbox_id}",
        )
        self.state.append_log(run_id, "Sandbox ready in 742ms · 2 vCPU · 8 GB")
        self.state.set_step(run_id, "sandbox", "complete")
        self.state.set_step(run_id, "runner", "active")
        self._pause()
        self.state.append_log(run_id, "Requesting just-in-time runner configuration")
        self.state.append_log(run_id, "Runner registered with GitHub · ephemeral=true")
        self.state.set_step(run_id, "runner", "complete")
        self.state.set_step(run_id, "workflow", "active")
        self.state.update_run(run_id, status="running")
        self._pause()
        self.state.append_log(run_id, "[checkout] Fetching acme/api-gateway@refs/pull/42/merge")
        self._pause()
        self.state.append_log(run_id, "[smoke] Tensorlake runner is online")
        self.state.append_log(run_id, "[smoke] Linux runner-h7k2 6.8.0 x86_64 GNU/Linux")
        self._pause()
        self.state.append_log(run_id, "Job completed successfully")
        self.state.set_step(run_id, "workflow", "complete")
        self.state.update_run(run_id, status="completed", conclusion="success")


class TensorlakeRunnerProvider:
    """Provision GitHub JIT runners inside ephemeral Tensorlake sandboxes."""

    def __init__(
        self,
        state: GoldenPathState,
        github: GitHubAppClient,
        installation_id: int,
    ):
        self.state = state
        self.github = github
        self.installation_id = installation_id
        self.image = os.getenv(
            "TENSORLAKE_CI_RUNNER_IMAGE", "tensorlake-ci-runner-ubuntu-2404"
        )
        self.runner_group_id = int(os.getenv("GITHUB_RUNNER_GROUP_ID", "1"))
        self._sandboxes: dict[str, Any] = {}

    def start_for_job(self, payload: dict[str, Any]) -> dict[str, Any]:
        job = payload["workflow_job"]
        repository = payload["repository"]["full_name"]
        workflow = payload.get("workflow", {}).get("name") or job.get("name", "Workflow")
        existing = self.state.find_run_for_job(job["id"])
        if existing:
            return existing
        run = self.state.claim_waiting_run(repository, job["id"], job.get("run_id"))
        if run is None:
            run = self.state.create_run(repository, workflow)
            self.state.bind_github_job(run["id"], job["id"], job.get("run_id"))
        threading.Thread(
            target=self._provision,
            args=(run["id"], payload),
            daemon=True,
        ).start()
        return run

    @staticmethod
    def _resources(labels: list[str]) -> tuple[float, int]:
        for label in labels:
            if label.startswith("tensorlake-") and "vcpu" in label:
                try:
                    cpus = int(label.split("-")[1].replace("vcpu", ""))
                    return float(cpus), cpus * 4096
                except (IndexError, ValueError):
                    break
        return 2.0, 8192

    def _provision(self, run_id: str, payload: dict[str, Any]) -> None:
        sandbox = None
        try:
            from tensorlake.sandbox import Sandbox

            job = payload["workflow_job"]
            organization = payload["organization"]["login"]
            labels = [
                label for label in job.get("labels", []) if label.startswith("tensorlake-")
            ]
            runner_name = f"tl-{job['id']}-{run_id[-4:]}"
            cpus, memory_mb = self._resources(labels)
            self.state.set_step(run_id, "queued", "complete")
            self.state.set_step(run_id, "sandbox", "active")
            self.state.update_run(run_id, status="provisioning")
            self.state.append_log(run_id, "Creating isolated Tensorlake sandbox")
            sandbox = Sandbox.create(
                image=self.image,
                cpus=cpus,
                memory_mb=memory_mb,
                timeout_secs=3600,
                allow_internet_access=True,
            )
            sandbox_id = sandbox.sandbox_id
            self._sandboxes[run_id] = sandbox
            self.state.update_run(
                run_id,
                sandbox={
                    "id": sandbox_id,
                    "status": "running",
                    "image": self.image,
                    "cpus": cpus,
                    "memory_mb": memory_mb,
                    "ephemeral": True,
                },
                ssh_command=f"tl sbx ssh {sandbox_id}",
            )
            self.state.set_step(run_id, "sandbox", "complete")
            self.state.set_step(run_id, "runner", "active")
            jit_config = self.github.generate_jit_config(
                self.installation_id,
                organization,
                runner_name,
                labels,
                self.runner_group_id,
            )
            process = sandbox.start_process(
                "/opt/actions-runner/run.sh",
                ["--jitconfig", jit_config],
                working_dir="/opt/actions-runner",
                name="github-actions-runner",
            )
            self.state.append_log(run_id, "JIT runner registered with GitHub")
            self.state.set_step(run_id, "runner", "complete")
            self.state.set_step(run_id, "workflow", "active")
            self.state.update_run(run_id, status="running")
            for event in sandbox.follow_output(process.pid):
                message = getattr(event, "line", None)
                if message:
                    self.state.append_log(run_id, message)
            info = sandbox.get_process(process.pid)
            exit_code = getattr(info, "exit_code", 0)
            conclusion = "success" if exit_code in (0, None) else "failure"
            self.state.set_step(run_id, "workflow", "complete")
            self.state.update_run(
                run_id, status="completed", conclusion=conclusion
            )
        except Exception as exc:
            self.state.append_log(run_id, str(exc), stream="stderr")
            self.state.update_run(
                run_id, status="completed", conclusion="failure", error=str(exc)
            )
        finally:
            run = self.state.get_run(run_id)
            if sandbox is not None and run and not run.get("retained"):
                try:
                    sandbox.terminate()
                except Exception:
                    pass
                self._sandboxes.pop(run_id, None)
