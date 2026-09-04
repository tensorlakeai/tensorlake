from __future__ import annotations

import copy
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


DEMO_REPOSITORIES = [
    {
        "id": 1001,
        "name": "api-gateway",
        "full_name": "acme/api-gateway",
        "private": True,
        "language": "TypeScript",
        "updated_at": "4 min ago",
        "default_branch": "main",
        "workflow_count": 2,
    },
    {
        "id": 1002,
        "name": "web-app",
        "full_name": "acme/web-app",
        "private": True,
        "language": "TypeScript",
        "updated_at": "yesterday",
        "default_branch": "main",
        "workflow_count": 1,
    },
    {
        "id": 1003,
        "name": "payments-service",
        "full_name": "acme/payments-service",
        "private": True,
        "language": "Go",
        "updated_at": "3 days ago",
        "default_branch": "main",
        "workflow_count": 1,
    },
]


@dataclass
class GoldenPathState:
    """Thread-safe, single-workspace state used by the prototype control plane."""

    mode: str = "demo"
    connected: bool = False
    installation_id: int | None = None
    organization: dict[str, Any] | None = None
    selected_repositories: list[str] = field(default_factory=list)
    migration_plan: dict[str, Any] | None = None
    pull_request: dict[str, Any] | None = None
    runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    oauth_states: set[str] = field(default_factory=set)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def connect_demo(self) -> dict[str, Any]:
        with self.lock:
            self.connected = True
            self.installation_id = 48151623
            self.organization = {
                "login": "acme",
                "name": "Acme Systems",
                "avatar": "A",
            }
            return copy.deepcopy(self.organization)

    def create_oauth_state(self) -> str:
        value = uuid.uuid4().hex
        with self.lock:
            self.oauth_states.add(value)
        return value

    def consume_oauth_state(self, value: str) -> bool:
        with self.lock:
            if value not in self.oauth_states:
                return False
            self.oauth_states.remove(value)
            return True

    def set_installation(
        self, installation_id: int, organization: dict[str, Any]
    ) -> None:
        with self.lock:
            self.connected = True
            self.installation_id = installation_id
            self.organization = copy.deepcopy(organization)

    def set_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.selected_repositories = list(plan.get("repositories", []))
            self.migration_plan = copy.deepcopy(plan)
            return copy.deepcopy(plan)

    def set_pull_request(self, pull_request: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self.pull_request = copy.deepcopy(pull_request)
            return copy.deepcopy(pull_request)

    def create_run(self, repository: str, workflow: str) -> dict[str, Any]:
        run_id = f"run_{uuid.uuid4().hex[:10]}"
        now = time.time()
        run = {
            "id": run_id,
            "repository": repository,
            "workflow": workflow,
            "status": "queued",
            "conclusion": None,
            "created_at": now,
            "updated_at": now,
            "sandbox": None,
            "ssh_command": None,
            "retained": False,
            "steps": [
                {"id": "queued", "name": "Queued by GitHub", "status": "active"},
                {"id": "sandbox", "name": "Provision sandbox", "status": "pending"},
                {"id": "runner", "name": "Register JIT runner", "status": "pending"},
                {"id": "workflow", "name": "Execute workflow", "status": "pending"},
            ],
            "logs": [],
        }
        with self.lock:
            self.runs[run_id] = run
            return copy.deepcopy(run)

    def find_run_for_job(self, github_job_id: int) -> dict[str, Any] | None:
        with self.lock:
            for run in self.runs.values():
                if run.get("github_job_id") == github_job_id:
                    return copy.deepcopy(run)
        return None

    def claim_waiting_run(
        self,
        repository: str,
        github_job_id: int,
        github_run_id: int | None = None,
    ) -> dict[str, Any] | None:
        """Atomically attach a GitHub job to the UI-created waiting run."""
        with self.lock:
            candidates = [
                run
                for run in self.runs.values()
                if run["repository"] == repository
                and run["status"] == "queued"
                and run.get("github_job_id") is None
            ]
            if not candidates:
                return None
            run = max(candidates, key=lambda item: item["created_at"])
            run["github_job_id"] = github_job_id
            if github_run_id is not None:
                run["github_run_id"] = github_run_id
            run["updated_at"] = time.time()
            return copy.deepcopy(run)

    def bind_github_job(
        self, run_id: str, github_job_id: int, github_run_id: int | None = None
    ) -> None:
        with self.lock:
            run = self.runs[run_id]
            run["github_job_id"] = github_job_id
            if github_run_id is not None:
                run["github_run_id"] = github_run_id
            run["updated_at"] = time.time()

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        with self.lock:
            run = self.runs[run_id]
            run.update(copy.deepcopy(changes))
            run["updated_at"] = time.time()
            return copy.deepcopy(run)

    def set_step(self, run_id: str, step_id: str, status: str) -> None:
        with self.lock:
            for step in self.runs[run_id]["steps"]:
                if step["id"] == step_id:
                    step["status"] = status
                    break
            self.runs[run_id]["updated_at"] = time.time()

    def append_log(self, run_id: str, message: str, stream: str = "stdout") -> None:
        with self.lock:
            self.runs[run_id]["logs"].append(
                {
                    "offset": len(self.runs[run_id]["logs"]),
                    "timestamp": time.time(),
                    "stream": stream,
                    "message": message,
                }
            )
            self.runs[run_id]["updated_at"] = time.time()

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.lock:
            value = self.runs.get(run_id)
            return copy.deepcopy(value) if value else None

    def latest_run(self) -> dict[str, Any] | None:
        with self.lock:
            if not self.runs:
                return None
            value = max(self.runs.values(), key=lambda run: run["created_at"])
            return copy.deepcopy(value)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "mode": self.mode,
                "connected": self.connected,
                "organization": copy.deepcopy(self.organization),
                "selected_repositories": list(self.selected_repositories),
                "migration_plan": copy.deepcopy(self.migration_plan),
                "pull_request": copy.deepcopy(self.pull_request),
                "latest_run": self.latest_run(),
            }

    def reset_demo(self) -> None:
        with self.lock:
            self.connected = False
            self.installation_id = None
            self.organization = None
            self.selected_repositories.clear()
            self.migration_plan = None
            self.pull_request = None
            self.runs.clear()
            self.oauth_states.clear()
