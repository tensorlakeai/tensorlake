from __future__ import annotations

import argparse
import os
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


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _venv_scripts_dir(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts"
    return venv_dir / "bin"


def _script_candidates(scripts_dir: Path, name: str) -> list[Path]:
    candidates = [scripts_dir / name]
    if os.name == "nt":
        candidates.extend(
            [
                scripts_dir / f"{name}.exe",
                scripts_dir / f"{name}.cmd",
                scripts_dir / f"{name}.bat",
            ]
        )
    return candidates


def _find_venv_script(venv_dir: Path, name: str) -> Path | None:
    scripts_dir = _venv_scripts_dir(venv_dir)
    return next(
        (
            candidate
            for candidate in _script_candidates(scripts_dir, name)
            if candidate.exists()
        ),
        None,
    )


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
        "--expect-script",
        action="append",
        default=[],
        metavar="NAME",
        help="Require an installed script with NAME in the wheel's virtualenv",
    )
    parser.add_argument(
        "--reject-script",
        action="append",
        default=[],
        metavar="NAME",
        help="Require that no installed script with NAME exists in the wheel's virtualenv",
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

        for script_name in args.expect_script:
            script_path = _find_venv_script(venv_dir, script_name)
            if script_path is None:
                raise SystemExit(
                    f"Script check FAILED: expected {script_name!r} in {_venv_scripts_dir(venv_dir)}"
                )
            print(f"Verified installed script: {script_path}")

        for script_name in args.reject_script:
            script_path = _find_venv_script(venv_dir, script_name)
            if script_path is not None:
                raise SystemExit(
                    f"Script check FAILED: unexpected {script_name!r} installed at {script_path}"
                )
            print(f"Verified script is absent: {script_name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
