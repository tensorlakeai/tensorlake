from __future__ import annotations

import argparse
import re
from pathlib import Path

DEFAULT_FILES = (
    "pyproject.toml",
    "Cargo.toml",
    "crates/rust-cloud-sdk-py/pyproject.toml",
)


def bump_version(path: Path, version: str) -> None:
    content = path.read_text()
    updated = re.sub(
        r'^version = "[^"]*"',
        f'version = "{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if updated == content:
        raise SystemExit(f"failed to update version in {path}")
    path.write_text(updated)
    print(f"Updated {path}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Bump Tensorlake package versions across release metadata files."
    )
    parser.add_argument("version", help="Version string to write")
    parser.add_argument(
        "files",
        nargs="*",
        default=list(DEFAULT_FILES),
        help="Files to update. Defaults to the standard release metadata files.",
    )
    args = parser.parse_args()

    for file_name in args.files:
        bump_version(Path(file_name), args.version)

    print(f"Version bumped to {args.version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
