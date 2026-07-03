from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
import venv
from pathlib import Path

# Runs inside the wheel's venv. Distinguishes a healthy TLS stack (the request
# reaches the API and fails auth) from miscompiled ring crypto in the Rust
# extension (connection error such as "invalid peer certificate: BadSignature").
_TLS_CHECK_SCRIPT = """\
import sys

from tensorlake._cloud_sdk import CloudApiClient, CloudApiClientError

url = sys.argv[1]
client = CloudApiClient(api_url=url, api_key="invalid")
try:
    client.introspect_api_key_json()
except CloudApiClientError as exc:
    kind = exc.args[0] if exc.args else None
    if kind in ("sdk_usage", "remote_api"):
        print(f"TLS check passed: got HTTP-level error {exc.args[:2]} from {url}")
        sys.exit(0)
    print(f"TLS check FAILED: expected an HTTP auth error from {url}, got: {exc!r}")
    sys.exit(1)
finally:
    client.close()
print(f"TLS check passed: request to {url} succeeded")
"""

_TLS_ERROR_SIGNATURES = (
    "badsignature",
    "invalid peer certificate",
    "error sending request",
    "connection",
)


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _venv_scripts_dir(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts"
    return venv_dir / "bin"


def _run_cli_check(venv_dir: Path, url: str) -> None:
    scripts_dir = _venv_scripts_dir(venv_dir)
    search_path = os.pathsep.join([str(scripts_dir), os.environ.get("PATH", "")])
    cli = shutil.which("tensorlake", path=search_path)
    if cli is None or not Path(cli).is_relative_to(venv_dir):
        raise SystemExit(f"CLI check FAILED: no tensorlake executable in {scripts_dir}")

    env = {k: v for k, v in os.environ.items() if not k.startswith("TENSORLAKE_")}
    proc = subprocess.run(
        [cli, "--api-key", "invalid", "--api-url", url, "whoami"],
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    output = proc.stdout + proc.stderr
    lowered = output.lower()
    matched = [s for s in _TLS_ERROR_SIGNATURES if s in lowered]
    if matched:
        raise SystemExit(
            f"CLI check FAILED: TLS/connection error signatures {matched} in output:\n{output}"
        )
    if proc.returncode == 0:
        raise SystemExit(
            f"CLI check FAILED: expected auth failure for invalid API key, got exit 0:\n{output}"
        )
    print(f"CLI check passed: auth failure without TLS errors (exit {proc.returncode})")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install a built wheel into a temporary virtualenv and verify imports."
    )
    parser.add_argument(
        "wheel_glob",
        help="Glob that resolves to exactly one wheel, for example dist/tensorlake-*.whl",
    )
    parser.add_argument(
        "modules",
        nargs="+",
        help="Modules to import after installation",
    )
    parser.add_argument(
        "--tls-check",
        metavar="URL",
        help="Make an HTTPS request to URL through the Rust _cloud_sdk extension and "
        "require an HTTP-level auth error (catches miscompiled TLS crypto)",
    )
    parser.add_argument(
        "--cli-check",
        metavar="URL",
        help="Run the wheel-bundled tensorlake CLI against URL with an invalid API key "
        "and require an auth error rather than a TLS/connection error",
    )
    args = parser.parse_args()

    matches = sorted(Path().glob(args.wheel_glob))
    if len(matches) != 1:
        raise SystemExit(
            f"Expected exactly one wheel matching {args.wheel_glob!r}, found {len(matches)}: {matches}"
        )

    wheel_path = matches[0].resolve()

    with tempfile.TemporaryDirectory(prefix="tensorlake-wheel-verify-") as tmpdir:
        venv_dir = Path(tmpdir) / "venv"
        venv.EnvBuilder(with_pip=True).create(venv_dir)
        python = _venv_python(venv_dir)

        subprocess.run(
            [str(python), "-m", "pip", "install", "--upgrade", "pip"],
            check=True,
        )
        subprocess.run(
            [str(python), "-m", "pip", "install", str(wheel_path)],
            check=True,
        )

        import_script = "\n".join(
            [
                *(f"import {module}" for module in args.modules),
                f'print("Verified imports:", {args.modules!r})',
            ]
        )
        subprocess.run(
            [
                str(python),
                "-c",
                import_script,
            ],
            check=True,
        )

        if args.tls_check:
            subprocess.run(
                [str(python), "-c", _TLS_CHECK_SCRIPT, args.tls_check],
                check=True,
            )

        if args.cli_check:
            _run_cli_check(venv_dir, args.cli_check)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
