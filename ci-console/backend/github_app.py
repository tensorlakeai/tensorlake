from __future__ import annotations

import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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
    api_version: str = "2022-11-28"

    @classmethod
    def from_env(cls) -> "GitHubConfig":
        values = {
            "app_id": os.getenv("GITHUB_APP_ID", ""),
            "app_slug": os.getenv("GITHUB_APP_SLUG", ""),
            "private_key_path": os.getenv("GITHUB_APP_PRIVATE_KEY_PATH", ""),
            "webhook_secret": os.getenv("GITHUB_WEBHOOK_SECRET", ""),
            "api_version": os.getenv("GITHUB_API_VERSION", "2022-11-28"),
        }
        missing = [
            key
            for key in ("app_id", "app_slug", "private_key_path", "webhook_secret")
            if not values[key]
        ]
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

    def installation_url(self) -> str:
        return f"https://github.com/apps/{self.config.app_slug}/installations/new"

    def _app_jwt(self) -> str:
        now = int(time.time())
        header = _b64url(b'{"alg":"RS256","typ":"JWT"}')
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
        payload: dict[str, Any] | None = None,
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
                "User-Agent": "tensorlake-ci/0.2",
                "X-GitHub-Api-Version": self.config.api_version,
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
        if account.get("type") != "Organization":
            raise GitHubError(
                "Tensorlake CI must be installed on a GitHub organization"
            )
        return {
            "login": account["login"],
            "name": account.get("name") or account["login"],
            "avatar_url": account.get("avatar_url"),
        }

    def generate_jit_config(
        self,
        *,
        installation_id: int,
        organization: str,
        name: str,
        labels: list[str],
        runner_group_id: int,
    ) -> str:
        result = self._request(
            "POST",
            (
                f"/orgs/{urllib.parse.quote(organization)}"
                "/actions/runners/generate-jitconfig"
            ),
            token=self.installation_token(installation_id),
            payload={
                "name": name,
                "runner_group_id": runner_group_id,
                "labels": labels,
                "work_folder": "_work",
            },
        )
        return result["encoded_jit_config"]
