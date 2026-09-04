from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from difflib import unified_diff
from pathlib import Path
from typing import Any


class GitHubError(RuntimeError):
    pass


@dataclass(frozen=True)
class GitHubConfig:
    app_id: str
    app_slug: str
    private_key_path: str
    webhook_secret: str
    callback_url: str

    @classmethod
    def from_env(cls) -> "GitHubConfig":
        values = {
            "app_id": os.getenv("GITHUB_APP_ID", ""),
            "app_slug": os.getenv("GITHUB_APP_SLUG", ""),
            "private_key_path": os.getenv("GITHUB_APP_PRIVATE_KEY_PATH", ""),
            "webhook_secret": os.getenv("GITHUB_WEBHOOK_SECRET", ""),
            "callback_url": os.getenv(
                "GITHUB_APP_CALLBACK_URL",
                "http://127.0.0.1:4173/api/github/callback",
            ),
        }
        missing = [key for key, value in values.items() if not value]
        if missing:
            raise GitHubError(
                "Live mode is missing GitHub configuration: " + ", ".join(missing)
            )
        if not Path(values["private_key_path"]).is_file():
            raise GitHubError("GITHUB_APP_PRIVATE_KEY_PATH does not point to a file")
        return cls(**values)


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


class GitHubAppClient:
    API_ROOT = "https://api.github.com"

    def __init__(self, config: GitHubConfig):
        self.config = config
        self._token_cache: dict[int, tuple[str, float]] = {}

    def installation_url(self, state: str) -> str:
        query = urllib.parse.urlencode({"state": state})
        return f"https://github.com/apps/{self.config.app_slug}/installations/new?{query}"

    def _app_jwt(self) -> str:
        now = int(time.time())
        header = _b64url(json.dumps({"alg": "RS256", "typ": "JWT"}).encode())
        payload = _b64url(
            json.dumps(
                {"iat": now - 30, "exp": now + 540, "iss": self.config.app_id},
                separators=(",", ":"),
            ).encode()
        )
        signing_input = f"{header}.{payload}".encode()
        try:
            result = subprocess.run(
                [
                    "openssl",
                    "dgst",
                    "-sha256",
                    "-sign",
                    self.config.private_key_path,
                ],
                input=signing_input,
                capture_output=True,
                check=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise GitHubError("Unable to sign GitHub App JWT with OpenSSL") from exc
        return f"{header}.{payload}.{_b64url(result.stdout)}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        token: str | None = None,
        payload: dict[str, Any] | list[Any] | None = None,
    ) -> Any:
        body = json.dumps(payload).encode() if payload is not None else None
        request = urllib.request.Request(
            f"{self.API_ROOT}{path}",
            data=body,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token or self._app_jwt()}",
                "Content-Type": "application/json",
                "User-Agent": "tensorlake-ci/0.1",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                content = response.read()
                return json.loads(content) if content else None
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise GitHubError(
                f"GitHub API {method} {path} failed ({exc.code}): {detail[:500]}"
            ) from exc
        except urllib.error.URLError as exc:
            raise GitHubError(f"GitHub API is unreachable: {exc.reason}") from exc

    def installation_token(self, installation_id: int) -> str:
        cached = self._token_cache.get(installation_id)
        if cached and cached[1] > time.time() + 60:
            return cached[0]
        result = self._request(
            "POST", f"/app/installations/{installation_id}/access_tokens"
        )
        token = result["token"]
        self._token_cache[installation_id] = (token, time.time() + 50 * 60)
        return token

    def installation(self, installation_id: int) -> dict[str, Any]:
        result = self._request("GET", f"/app/installations/{installation_id}")
        account = result["account"]
        return {
            "login": account["login"],
            "name": account.get("name") or account["login"],
            "avatar_url": account.get("avatar_url"),
            "avatar": account["login"][0].upper(),
        }

    def repositories(self, installation_id: int) -> list[dict[str, Any]]:
        token = self.installation_token(installation_id)
        result = self._request(
            "GET", "/installation/repositories?per_page=100", token=token
        )
        repositories = []
        for repo in result.get("repositories", []):
            repositories.append(
                {
                    "id": repo["id"],
                    "name": repo["name"],
                    "full_name": repo["full_name"],
                    "private": repo["private"],
                    "language": repo.get("language") or "Repository",
                    "updated_at": repo.get("updated_at"),
                    "default_branch": repo["default_branch"],
                    "workflow_count": None,
                }
            )
        return repositories

    def _repo_request(
        self,
        installation_id: int,
        method: str,
        full_name: str,
        suffix: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        owner, repo = full_name.split("/", 1)
        return self._request(
            method,
            f"/repos/{owner}/{repo}{suffix}",
            token=self.installation_token(installation_id),
            payload=payload,
        )

    def workflow_files(
        self, installation_id: int, full_name: str, ref: str
    ) -> list[dict[str, str]]:
        try:
            entries = self._repo_request(
                installation_id,
                "GET",
                full_name,
                f"/contents/.github/workflows?ref={urllib.parse.quote(ref)}",
            )
        except GitHubError as exc:
            if "(404)" in str(exc):
                return []
            raise
        files = []
        for entry in entries:
            if entry.get("type") != "file" or not entry["name"].endswith(
                (".yml", ".yaml")
            ):
                continue
            content = self._repo_request(
                installation_id,
                "GET",
                full_name,
                f"/contents/{entry['path']}?ref={urllib.parse.quote(ref)}",
            )
            decoded = base64.b64decode(content["content"]).decode(
                "utf-8", errors="replace"
            )
            files.append(
                {
                    "path": entry["path"],
                    "sha": content["sha"],
                    "content": decoded,
                }
            )
        return files

    @staticmethod
    def migrate_workflow(source: str) -> tuple[str, int]:
        mappings = {
            "ubuntu-latest": "tensorlake-2vcpu-ubuntu-2404",
            "ubuntu-24.04": "tensorlake-2vcpu-ubuntu-2404",
            "ubuntu-22.04": "tensorlake-2vcpu-ubuntu-2204",
        }
        changed = 0
        pattern = re.compile(
            r"^(?P<indent>\s*runs-on:\s*)(?P<quote>['\"]?)(?P<label>ubuntu-(?:latest|24\.04|22\.04))(?P=quote)(?P<tail>\s*(?:#.*)?)$",
            re.MULTILINE,
        )

        def replace(match: re.Match[str]) -> str:
            nonlocal changed
            changed += 1
            return (
                f"{match.group('indent')}{match.group('quote')}"
                f"{mappings[match.group('label')]}{match.group('quote')}"
                f"{match.group('tail')}"
            )

        return pattern.sub(replace, source), changed

    def plan_migration(
        self, installation_id: int, repositories: list[dict[str, Any]]
    ) -> dict[str, Any]:
        changes: list[dict[str, Any]] = []
        unsupported: list[dict[str, str]] = []
        for repository in repositories:
            full_name = repository["full_name"]
            branch = repository["default_branch"]
            workflows = self.workflow_files(installation_id, full_name, branch)
            for workflow in workflows:
                migrated, count = self.migrate_workflow(workflow["content"])
                if count == 0:
                    if "runs-on:" in workflow["content"]:
                        unsupported.append(
                            {
                                "repository": full_name,
                                "path": workflow["path"],
                                "reason": "No supported GitHub-hosted Linux labels found",
                            }
                        )
                    continue
                diff = "\n".join(
                    unified_diff(
                        workflow["content"].splitlines(),
                        migrated.splitlines(),
                        fromfile=f"a/{workflow['path']}",
                        tofile=f"b/{workflow['path']}",
                        lineterm="",
                    )
                )
                changes.append(
                    {
                        "repository": full_name,
                        "default_branch": branch,
                        "path": workflow["path"],
                        "sha": workflow["sha"],
                        "original": workflow["content"],
                        "migrated": migrated,
                        "replacements": count,
                        "diff": diff,
                    }
                )
        return {
            "repositories": [repo["full_name"] for repo in repositories],
            "changes": changes,
            "unsupported": unsupported,
            "workflow_count": len(changes),
            "replacement_count": sum(item["replacements"] for item in changes),
        }

    def create_migration_pr(
        self,
        installation_id: int,
        plan: dict[str, Any],
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for change in plan["changes"]:
            grouped.setdefault(change["repository"], []).append(change)
        pull_requests = []
        for full_name, changes in grouped.items():
            default_branch = changes[0]["default_branch"]
            branch = f"tensorlake-ci/migrate-{uuid.uuid4().hex[:8]}"
            base_ref = self._repo_request(
                installation_id,
                "GET",
                full_name,
                f"/git/ref/heads/{urllib.parse.quote(default_branch)}",
            )
            self._repo_request(
                installation_id,
                "POST",
                full_name,
                "/git/refs",
                {"ref": f"refs/heads/{branch}", "sha": base_ref["object"]["sha"]},
            )
            for change in changes:
                self._repo_request(
                    installation_id,
                    "PUT",
                    full_name,
                    f"/contents/{change['path']}",
                    {
                        "message": f"ci: run {Path(change['path']).name} on Tensorlake",
                        "content": base64.b64encode(change["migrated"].encode()).decode(),
                        "sha": change["sha"],
                        "branch": branch,
                    },
                )
            smoke_path = ".github/workflows/tensorlake-ci-smoke.yml"
            smoke_source = (
                "name: Tensorlake CI smoke test\n"
                "on:\n"
                "  pull_request:\n"
                "    paths:\n"
                "      - '.github/workflows/tensorlake-ci-smoke.yml'\n"
                "jobs:\n"
                "  smoke:\n"
                "    runs-on: tensorlake-2vcpu-ubuntu-2404\n"
                "    steps:\n"
                "      - uses: actions/checkout@v4\n"
                "      - name: Verify Tensorlake runner\n"
                "        run: |\n"
                "          echo 'Tensorlake runner is online'\n"
                "          uname -a\n"
            )
            self._repo_request(
                installation_id,
                "PUT",
                full_name,
                f"/contents/{smoke_path}",
                {
                    "message": "ci: add Tensorlake runner smoke test",
                    "content": base64.b64encode(smoke_source.encode()).decode(),
                    "branch": branch,
                },
            )
            pr = self._repo_request(
                installation_id,
                "POST",
                full_name,
                "/pulls",
                {
                    "title": "Run CI on Tensorlake serverless runners",
                    "head": branch,
                    "base": default_branch,
                    "body": (
                        "This PR migrates supported GitHub-hosted Linux jobs to ephemeral "
                        "Tensorlake runners and adds a one-time smoke workflow.\n\n"
                        "Generated by Tensorlake CI. No application code was changed."
                    ),
                },
            )
            pull_requests.append(
                {
                    "repository": full_name,
                    "number": pr["number"],
                    "url": pr["html_url"],
                    "branch": branch,
                }
            )
        return pull_requests

    def generate_jit_config(
        self,
        installation_id: int,
        organization: str,
        name: str,
        labels: list[str],
        runner_group_id: int = 1,
    ) -> str:
        result = self._request(
            "POST",
            f"/orgs/{urllib.parse.quote(organization)}/actions/runners/generate-jitconfig",
            token=self.installation_token(installation_id),
            payload={
                "name": name,
                "runner_group_id": runner_group_id,
                "labels": labels,
                "work_folder": "_work",
            },
        )
        return result["encoded_jit_config"]
