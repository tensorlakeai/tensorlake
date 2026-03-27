from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
import venv
from pathlib import Path


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


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
            [*(f"import {module}" for module in args.modules), f'print("Verified imports:", {args.modules!r})']
        )
        subprocess.run(
            [
                str(python),
                "-c",
                import_script,
            ],
            check=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
