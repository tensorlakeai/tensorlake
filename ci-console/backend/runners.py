from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from .github_app import GitHubAppClient
from .state import RunnerState


@dataclass(frozen=True)
class RunnerProfile:
    label: str
    cpus: float
    memory_mb: int


RUNNER_PROFILES = {
    profile.label: profile
    for profile in (
        RunnerProfile("tensorlake", 2.0, 8192),
        RunnerProfile("tensorlake-2vcpu-ubuntu-2404", 2.0, 8192),
        RunnerProfile("tensorlake-4vcpu-ubuntu-2404", 4.0, 16384),
        RunnerProfile("tensorlake-8vcpu-ubuntu-2404", 8.0, 32768),
    )
}


def resolve_profile(labels: list[str]) -> RunnerProfile | None:
    requested = [
        RUNNER_PROFILES[label.lower()]
        for label in labels
        if label.lower() in RUNNER_PROFILES
    ]
    if not requested:
        return None
    return requested[0]


def runner_labels(job_labels: list[str], profile: RunnerProfile) -> list[str]:
    labels = ["self-hosted", "linux", "x64", profile.label, *job_labels]
    return list(dict.fromkeys(label.lower() for label in labels))


class DemoRunnerProvider:
    def __init__(self, state: RunnerState, delay: float = 0.2):
        self.state = state
        self.delay = delay

    def start_for_job(
        self,
        payload: dict[str, Any],
        installation_id: int,
        profile: RunnerProfile,
    ) -> dict[str, Any]:
        del installation_id
        job = payload["workflow_job"]
        run, created = self.state.get_or_create_run(
            github_job_id=job["id"],
            github_run_id=job.get("run_id"),
            repository=payload["repository"]["full_name"],
            workflow=job.get("name", "GitHub Actions job"),
            label=profile.label,
        )
        if created:
            threading.Thread(
                target=self._execute,
                args=(run["id"], profile),
                daemon=True,
            ).start()
        return run

    def _execute(self, run_id: str, profile: RunnerProfile) -> None:
        self.state.update_run(run_id, status="provisioning")
        self.state.append_log(run_id, f"Matched runner label: {profile.label}")
        time.sleep(self.delay)
        sandbox_id = "sbx_demo_runner"
        self.state.update_run(
            run_id,
            status="running",
            sandbox={
                "id": sandbox_id,
                "status": "running",
                "cpus": profile.cpus,
                "memory_mb": profile.memory_mb,
            },
            ssh_command=f"tl sbx ssh {sandbox_id}",
        )
        self.state.append_log(run_id, "Sandbox ready; JIT runner registered")
        time.sleep(self.delay)
        self.state.append_log(run_id, "GitHub Actions job completed successfully")
        self.state.update_run(run_id, status="completed", conclusion="success")


class TensorlakeRunnerProvider:
    """Start one ephemeral Tensorlake sandbox for each matching queued job."""

    def __init__(self, state: RunnerState, github: GitHubAppClient):
        self.state = state
        self.github = github
        self.image = os.getenv(
            "TENSORLAKE_CI_RUNNER_IMAGE", "tensorlake-ci-runner-ubuntu-2404"
        )
        self.runner_group_id = int(os.getenv("GITHUB_RUNNER_GROUP_ID", "1"))
        self._sandboxes: dict[str, Any] = {}

    def start_for_job(
        self,
        payload: dict[str, Any],
        installation_id: int,
        profile: RunnerProfile,
    ) -> dict[str, Any]:
        job = payload["workflow_job"]
        run, created = self.state.get_or_create_run(
            github_job_id=job["id"],
            github_run_id=job.get("run_id"),
            repository=payload["repository"]["full_name"],
            workflow=job.get("name", "GitHub Actions job"),
            label=profile.label,
        )
        if created:
            threading.Thread(
                target=self._provision,
                args=(run["id"], payload, installation_id, profile),
                daemon=True,
            ).start()
        return run

    def _provision(
        self,
        run_id: str,
        payload: dict[str, Any],
        installation_id: int,
        profile: RunnerProfile,
    ) -> None:
        sandbox = None
        try:
            from tensorlake.sandbox import Sandbox

            job = payload["workflow_job"]
            organization = payload.get("organization", {}).get("login")
            if not organization:
                raise RuntimeError("Tensorlake CI requires a GitHub organization")

            self.state.update_run(run_id, status="provisioning")
            self.state.append_log(run_id, f"Matched runner label: {profile.label}")
            sandbox = Sandbox.create(
                image=self.image,
                cpus=profile.cpus,
                memory_mb=profile.memory_mb,
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
                    "cpus": profile.cpus,
                    "memory_mb": profile.memory_mb,
                },
                ssh_command=f"tl sbx ssh {sandbox_id}",
            )

            jit_config = self.github.generate_jit_config(
                installation_id=installation_id,
                organization=organization,
                name=f"tl-{job['id']}-{run_id[-4:]}",
                labels=runner_labels(job.get("labels", []), profile),
                runner_group_id=self.runner_group_id,
            )
            process = sandbox.start_process(
                "/opt/actions-runner/run.sh",
                ["--jitconfig", jit_config],
                working_dir="/opt/actions-runner",
                name="github-actions-runner",
            )
            self.state.update_run(run_id, status="running")
            self.state.append_log(run_id, "Sandbox ready; JIT runner registered")
            for event in sandbox.follow_output(process.pid):
                self.state.append_log(
                    run_id,
                    event.line,
                    stream=event.stream or "stdout",
                )
            info = sandbox.get_process(process.pid)
            conclusion = "success" if info.exit_code in (0, None) else "failure"
            self.state.update_run(
                run_id,
                status="completed",
                conclusion=conclusion,
            )
        except Exception as exc:
            self.state.append_log(run_id, str(exc), stream="stderr")
            self.state.update_run(
                run_id,
                status="completed",
                conclusion="failure",
                error=str(exc),
            )
        finally:
            if sandbox is not None:
                try:
                    sandbox.terminate()
                except Exception:
                    pass
                self._sandboxes.pop(run_id, None)
