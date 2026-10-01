from __future__ import annotations

import copy
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RunnerState:
    """Thread-safe state for webhook idempotency and local run inspection."""

    runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    runs_by_job: dict[int, str] = field(default_factory=dict)
    deliveries: set[str] = field(default_factory=set)
    lock: threading.RLock = field(default_factory=threading.RLock, repr=False)

    def accept_delivery(self, delivery_id: str) -> bool:
        if not delivery_id:
            return True
        with self.lock:
            if delivery_id in self.deliveries:
                return False
            self.deliveries.add(delivery_id)
            return True

    def get_or_create_run(
        self,
        *,
        github_job_id: int,
        github_run_id: int | None,
        repository: str,
        workflow: str,
        label: str,
    ) -> tuple[dict[str, Any], bool]:
        with self.lock:
            existing_id = self.runs_by_job.get(github_job_id)
            if existing_id is not None:
                return copy.deepcopy(self.runs[existing_id]), False

            run_id = f"run_{uuid.uuid4().hex[:10]}"
            now = time.time()
            run = {
                "id": run_id,
                "github_job_id": github_job_id,
                "github_run_id": github_run_id,
                "repository": repository,
                "workflow": workflow,
                "label": label,
                "status": "queued",
                "conclusion": None,
                "created_at": now,
                "updated_at": now,
                "sandbox": None,
                "ssh_command": None,
                "logs": [],
            }
            self.runs[run_id] = run
            self.runs_by_job[github_job_id] = run_id
            return copy.deepcopy(run), True

    def find_run_for_job(self, github_job_id: int) -> dict[str, Any] | None:
        with self.lock:
            run_id = self.runs_by_job.get(github_job_id)
            if run_id is None:
                return None
            return copy.deepcopy(self.runs[run_id])

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self.lock:
            run = self.runs.get(run_id)
            return copy.deepcopy(run) if run else None

    def list_runs(self) -> list[dict[str, Any]]:
        with self.lock:
            values = sorted(
                self.runs.values(), key=lambda run: run["created_at"], reverse=True
            )
            return copy.deepcopy(values)

    def update_run(self, run_id: str, **changes: Any) -> dict[str, Any]:
        with self.lock:
            run = self.runs[run_id]
            run.update(copy.deepcopy(changes))
            run["updated_at"] = time.time()
            return copy.deepcopy(run)

    def append_log(self, run_id: str, message: str, stream: str = "stdout") -> None:
        with self.lock:
            run = self.runs[run_id]
            run["logs"].append(
                {
                    "offset": len(run["logs"]),
                    "timestamp": time.time(),
                    "stream": stream,
                    "message": message,
                }
            )
            run["updated_at"] = time.time()
