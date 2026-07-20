"""Wrapper around the `tl` CLI for local mount operations.

Mounting a filesystem to a local path is served by a FUSE (Linux) / FSKit
(macOS) daemon that ships only inside the Tensorlake CLI, so the SDK drives
mounts by invoking ``tl fs mount/unmount/snapshot/status``. Everything else
(create, read, write, snapshot-by-write, remote status) goes over HTTP and
does not need the CLI.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from .exceptions import CliNotFoundError, MountError

_DEFAULT_INSTALL_PATH = Path.home() / ".tensorlake" / "bin" / "tl"


def find_cli() -> str:
    """Locate a `tl` binary that supports the `fs` command group."""
    candidates: List[str] = []
    env_path = os.environ.get("TENSORLAKE_CLI")
    if env_path:
        candidates.append(env_path)
    which = shutil.which("tl")
    if which:
        candidates.append(which)
    if _DEFAULT_INSTALL_PATH.is_file():
        candidates.append(str(_DEFAULT_INSTALL_PATH))
    # A candidate that exists but fails the probe means "upgrade tl"; a
    # candidate that does not exist must not be blamed as outdated. The
    # TypeScript SDK mirrors these exact semantics.
    unsupported: Optional[str] = None
    for candidate in candidates:
        try:
            probe = subprocess.run(
                [candidate, "fs", "--help"],
                capture_output=True,
                text=True,
                timeout=15,
            )
        except FileNotFoundError:
            continue
        except (OSError, subprocess.TimeoutExpired):
            unsupported = candidate
            continue
        if probe.returncode == 0:
            return candidate
        unsupported = candidate
    if unsupported is not None:
        raise CliNotFoundError(
            f"`tl` at {unsupported} does not support `tl fs` (upgrade required)"
        )
    raise CliNotFoundError("`tl` was not found on PATH")


class FsCli:
    """Runs `tl fs ...` commands with the client's credentials in the env."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        organization_id: Optional[str] = None,
        project_id: Optional[str] = None,
        api_url: Optional[str] = None,
    ):
        self._binary: Optional[str] = None
        self._env_overrides: Dict[str, str] = {}
        if api_key:
            self._env_overrides["TENSORLAKE_API_KEY"] = api_key
        if organization_id:
            self._env_overrides["TENSORLAKE_ORGANIZATION_ID"] = organization_id
        if project_id:
            self._env_overrides["TENSORLAKE_PROJECT_ID"] = project_id
        if api_url:
            # The CLI must target the same deployment the data plane does, or
            # a mount could resolve a same-named filesystem in the wrong
            # environment.
            self._env_overrides["TENSORLAKE_API_URL"] = api_url

    def _run(self, args: List[str], timeout: float = 300.0) -> str:
        if self._binary is None:
            self._binary = find_cli()
        env = dict(os.environ)
        env.update(self._env_overrides)
        try:
            result = subprocess.run(
                [self._binary, "fs", *args],
                capture_output=True,
                text=True,
                timeout=timeout,
                env=env,
            )
        except subprocess.TimeoutExpired as e:
            raise MountError(f"`tl fs {args[0]}` timed out after {timeout:.0f}s") from e
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()
            raise MountError(f"`tl fs {' '.join(args)}` failed: {detail}")
        return result.stdout

    # Flags always precede a `--` end-of-options separator so caller-supplied
    # names/paths can never be parsed as CLI flags (e.g. a path literally
    # named "--discard" must stay a path, not become the destructive flag).
    def mount(self, filesystem: str, local_path: str, readonly: bool) -> None:
        args = ["mount"]
        if readonly:
            args.append("--ro")
        args += ["--", filesystem, local_path]
        self._run(args)

    def unmount(self, local_path: str, discard: bool = False) -> None:
        args = ["unmount"]
        if discard:
            args.append("--discard")
        args += ["--", local_path]
        self._run(args)

    def snapshot(self, local_path: str, message: Optional[str]) -> None:
        args = ["snapshot"]
        if message:
            # Attached form: a detached value ("-m", "-msg") is rejected by
            # the CLI parser when the message starts with '-'.
            args.append(f"--message={message}")
        args += ["--", local_path]
        self._run(args)

    def status(self, local_path: Optional[str]) -> Dict[str, Any]:
        args = ["status", "--json"]
        if local_path:
            args += ["--", local_path]
        output = self._run(args, timeout=60.0)
        try:
            payload = json.loads(output)
        except json.JSONDecodeError as e:
            raise MountError(
                f"`tl fs status --json` returned invalid JSON: {output[:200]!r}"
            ) from e
        if not isinstance(payload, dict):
            payload = {"status": payload}
        return payload
