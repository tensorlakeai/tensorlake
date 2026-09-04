from __future__ import annotations

import hashlib
import hmac
import threading
from typing import Any

from .github_app import GitHubAppClient, GitHubConfig, GitHubError
from .runners import DemoRunnerProvider, TensorlakeRunnerProvider
from .state import DEMO_REPOSITORIES, GoldenPathState


class GoldenPathController:
    def __init__(self, mode: str = "demo", demo_delay: float = 0.35):
        if mode not in {"demo", "live"}:
            raise ValueError("mode must be 'demo' or 'live'")
        self.state = GoldenPathState(mode=mode)
        self.github: GitHubAppClient | None = None
        self.runner: DemoRunnerProvider | TensorlakeRunnerProvider
        if mode == "live":
            self.github = GitHubAppClient(GitHubConfig.from_env())
            self.runner = DemoRunnerProvider(self.state, demo_delay)
        else:
            self.runner = DemoRunnerProvider(self.state, demo_delay)

    @property
    def mode(self) -> str:
        return self.state.mode

    def connect(self) -> dict[str, Any]:
        if self.mode == "demo":
            organization = self.state.connect_demo()
            return {"connected": True, "organization": organization}
        assert self.github is not None
        oauth_state = self.state.create_oauth_state()
        return {
            "connected": False,
            "redirect_url": self.github.installation_url(oauth_state),
        }

    def complete_installation(
        self, installation_id: int, oauth_state: str
    ) -> dict[str, Any]:
        if self.mode != "live" or self.github is None:
            raise GitHubError("GitHub installation callbacks require live mode")
        if not self.state.consume_oauth_state(oauth_state):
            raise GitHubError("GitHub installation state is invalid or expired")
        organization = self.github.installation(installation_id)
        self.state.set_installation(installation_id, organization)
        self.runner = TensorlakeRunnerProvider(
            self.state, self.github, installation_id
        )
        return organization

    def repositories(self) -> list[dict[str, Any]]:
        if not self.state.connected:
            raise GitHubError("Connect GitHub before listing repositories")
        if self.mode == "demo":
            return DEMO_REPOSITORIES
        assert self.github is not None and self.state.installation_id is not None
        return self.github.repositories(self.state.installation_id)

    def plan(self, repository_names: list[str]) -> dict[str, Any]:
        available = {repo["full_name"]: repo for repo in self.repositories()}
        selected = [available[name] for name in repository_names if name in available]
        if not selected:
            raise ValueError("Select at least one available repository")
        if self.mode == "demo":
            plan = self._demo_plan(selected)
        else:
            assert self.github is not None and self.state.installation_id is not None
            plan = self.github.plan_migration(self.state.installation_id, selected)
        if not plan["changes"]:
            raise ValueError("No supported GitHub-hosted Linux runner labels were found")
        return self.state.set_plan(plan)

    @staticmethod
    def _demo_plan(repositories: list[dict[str, Any]]) -> dict[str, Any]:
        changes = []
        for index, repo in enumerate(repositories):
            path = ".github/workflows/ci.yml" if index == 0 else ".github/workflows/preview.yml"
            old = "ubuntu-latest"
            new = "tensorlake-2vcpu-ubuntu-2404"
            changes.append(
                {
                    "repository": repo["full_name"],
                    "default_branch": repo["default_branch"],
                    "path": path,
                    "sha": f"demo-{index}",
                    "replacements": 1,
                    "diff": (
                        f"--- a/{path}\n+++ b/{path}\n@@ -7,3 +7,3 @@\n"
                        " jobs:\n   test:\n"
                        f"-    runs-on: {old}\n+    runs-on: {new}"
                    ),
                    "original": f"jobs:\n  test:\n    runs-on: {old}\n",
                    "migrated": f"jobs:\n  test:\n    runs-on: {new}\n",
                }
            )
        return {
            "repositories": [repo["full_name"] for repo in repositories],
            "changes": changes,
            "unsupported": [],
            "workflow_count": len(changes),
            "replacement_count": len(changes),
        }

    def create_pull_request(self) -> dict[str, Any]:
        plan = self.state.migration_plan
        if not plan:
            raise ValueError("Create a migration plan first")
        if self.mode == "demo":
            repositories = plan["repositories"]
            pull_requests = [
                {
                    "repository": repository,
                    "number": 42 + index,
                    "url": f"https://github.com/{repository}/pull/{42 + index}",
                    "branch": "tensorlake-ci/migrate-runners",
                }
                for index, repository in enumerate(repositories)
            ]
        else:
            assert self.github is not None and self.state.installation_id is not None
            pull_requests = self.github.create_migration_pr(
                self.state.installation_id, plan
            )
        result = {
            "status": "open",
            "pull_requests": pull_requests,
            "primary": pull_requests[0],
        }
        return self.state.set_pull_request(result)

    def start_smoke_run(self) -> dict[str, Any]:
        pull_request = self.state.pull_request
        if not pull_request:
            raise ValueError("Open the migration pull request first")
        primary = pull_request["primary"]
        if self.mode == "live":
            run = self.state.create_run(
                primary["repository"], "Tensorlake CI smoke test"
            )
            self.state.append_log(
                run["id"], "Waiting for GitHub to queue the smoke workflow..."
            )
            return run
        return self.runner.start(
            primary["repository"], "Tensorlake CI smoke test"
        )

    def verify_webhook(self, body: bytes, signature: str) -> bool:
        if self.mode != "live" or self.github is None:
            return True
        expected = "sha256=" + hmac.new(
            self.github.config.webhook_secret.encode(), body, hashlib.sha256
        ).hexdigest()
        return hmac.compare_digest(expected, signature)

    def handle_webhook(
        self, event: str, delivery_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        if event != "workflow_job":
            return {"accepted": True, "action": "ignored", "delivery": delivery_id}
        job = payload.get("workflow_job", {})
        action = payload.get("action")
        labels = job.get("labels", [])
        if not any(label.startswith("tensorlake-") for label in labels):
            return {"accepted": True, "action": "ignored", "delivery": delivery_id}
        if action == "queued" and isinstance(self.runner, TensorlakeRunnerProvider):
            run = self.runner.start_for_job(payload)
            return {"accepted": True, "action": "provisioning", "run_id": run["id"]}
        if action == "completed":
            run = self.state.find_run_for_job(job["id"])
            if run:
                self.state.set_step(run["id"], "workflow", "complete")
                self.state.update_run(
                    run["id"],
                    status="completed",
                    conclusion=job.get("conclusion"),
                )
            return {"accepted": True, "action": "completed", "delivery": delivery_id}
        return {"accepted": True, "action": "observed", "delivery": delivery_id}

    def retain_run(self, run_id: str) -> dict[str, Any]:
        run = self.state.get_run(run_id)
        if not run:
            raise KeyError(run_id)
        return self.state.update_run(run_id, retained=True)

    def reset(self) -> dict[str, Any]:
        if self.mode != "demo":
            raise ValueError("Live GitHub installations cannot be reset from the demo control")
        self.state.reset_demo()
        return self.state.snapshot()
