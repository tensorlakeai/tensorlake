from __future__ import annotations

import hashlib
import hmac
import itertools
from typing import Any

from .github_app import GitHubAppClient, GitHubConfig, GitHubError
from .runners import (
    RUNNER_PROFILES,
    DemoRunnerProvider,
    TensorlakeRunnerProvider,
    resolve_profile,
)
from .state import RunnerState


class RunnerController:
    def __init__(self, mode: str = "demo", demo_delay: float = 0.2):
        if mode not in {"demo", "live"}:
            raise ValueError("mode must be 'demo' or 'live'")
        self.mode = mode
        self.state = RunnerState()
        self.github: GitHubAppClient | None = None
        self._demo_job_ids = itertools.count(1000)
        if mode == "live":
            self.github = GitHubAppClient(GitHubConfig.from_env())
            self.runner = TensorlakeRunnerProvider(self.state, self.github)
        else:
            self.runner = DemoRunnerProvider(self.state, demo_delay)

    def configuration(self) -> dict[str, Any]:
        install_url = (
            self.github.installation_url()
            if self.github is not None
            else "/?installed=demo&account=acme"
        )
        return {
            "mode": self.mode,
            "install_url": install_url,
            "default_label": "tensorlake",
            "profiles": [
                {
                    "label": profile.label,
                    "cpus": profile.cpus,
                    "memory_mb": profile.memory_mb,
                }
                for profile in RUNNER_PROFILES.values()
            ],
        }

    def complete_installation(self, installation_id: int) -> dict[str, Any]:
        if self.github is None:
            return {"login": "acme", "name": "Acme"}
        return self.github.installation(installation_id)

    def verify_webhook(self, body: bytes, signature: str) -> bool:
        if self.github is None:
            return True
        expected = (
            "sha256="
            + hmac.new(
                self.github.config.webhook_secret.encode(), body, hashlib.sha256
            ).hexdigest()
        )
        return hmac.compare_digest(expected, signature)

    def handle_webhook(
        self,
        event: str,
        delivery_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if not self.state.accept_delivery(delivery_id):
            return {"accepted": True, "action": "duplicate", "delivery": delivery_id}
        if event != "workflow_job":
            return {"accepted": True, "action": "ignored", "delivery": delivery_id}

        job = payload.get("workflow_job", {})
        profile = resolve_profile(job.get("labels", []))
        if profile is None:
            return {"accepted": True, "action": "ignored", "delivery": delivery_id}

        action = payload.get("action")
        if action == "queued":
            installation_id = payload.get("installation", {}).get("id")
            if self.mode == "live" and not isinstance(installation_id, int):
                raise GitHubError("workflow_job payload is missing an installation id")
            run = self.runner.start_for_job(payload, installation_id or 0, profile)
            return {"accepted": True, "action": "provisioning", "run_id": run["id"]}

        if action == "completed" and isinstance(job.get("id"), int):
            run = self.state.find_run_for_job(job["id"])
            if run is not None:
                self.state.update_run(
                    run["id"],
                    status="completed",
                    conclusion=job.get("conclusion"),
                )
            return {"accepted": True, "action": "completed", "delivery": delivery_id}

        return {"accepted": True, "action": "observed", "delivery": delivery_id}

    def dispatch_demo_job(self, label: str = "tensorlake") -> dict[str, Any]:
        if self.mode != "demo":
            raise ValueError("Demo jobs are disabled in live mode")
        profile = resolve_profile([label])
        if profile is None:
            raise ValueError("Unsupported Tensorlake runner label")
        job_id = next(self._demo_job_ids)
        payload = {
            "action": "queued",
            "installation": {"id": 1},
            "organization": {"login": "acme"},
            "repository": {"full_name": "acme/api"},
            "workflow_job": {
                "id": job_id,
                "run_id": job_id + 10000,
                "name": "test",
                "labels": [label],
            },
        }
        return self.handle_webhook("workflow_job", f"demo-{job_id}", payload)
