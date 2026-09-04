from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

DEFAULT_FILES = (
    "pyproject.toml",
    "Cargo.toml",
    "crates/gsvc-fs-client/Cargo.toml",
    "crates/rust-cloud-sdk-py/pyproject.toml",
    "typescript/package.json",
    "typescript/package-lock.json",
    "typescript/npm/darwin-arm64/package.json",
    "typescript/npm/linux-arm64/package.json",
    "typescript/npm/linux-arm64-musl/package.json",
    "typescript/npm/linux-x64/package.json",
    "typescript/npm/linux-x64-musl/package.json",
    "typescript/npm/win32-x64/package.json",
)

NATIVE_PACKAGE_PREFIX = "@tensorlake/native-"


def bump_typescript_package_lock(path: Path, version: str) -> None:
    package_lock = json.loads(path.read_text())
    package_lock["version"] = version
    root_package = package_lock["packages"][""]
    root_package["version"] = version

    native_dependencies = root_package.get("optionalDependencies", {})
    if not native_dependencies:
        raise SystemExit(f"failed to find native dependency versions in {path}")

    for package_name in native_dependencies:
        if not package_name.startswith(NATIVE_PACKAGE_PREFIX):
            continue
        native_dependencies[package_name] = version
        lock_entry = package_lock["packages"].get(f"node_modules/{package_name}")
        if lock_entry is None:
            raise SystemExit(f"failed to find {package_name} in {path}")
        lock_entry["version"] = version
        # The new artifact does not exist yet when versions are bumped. Avoid
        # retaining the previous release's checksum under the new tarball URL.
        lock_entry.pop("integrity", None)
        archive_name = package_name.rsplit("/", 1)[-1]
        lock_entry["resolved"] = (
            f"https://registry.npmjs.org/{package_name}/-/"
            f"{archive_name}-{version}.tgz"
        )

    path.write_text(json.dumps(package_lock, indent=2) + "\n")
    print(f"Updated {path}")


def bump_version(path: Path, version: str) -> None:
    if path.as_posix() == "typescript/package-lock.json":
        bump_typescript_package_lock(path, version)
        return

    content = path.read_text()
    if path.suffix == ".json":
        pattern = r'"version":\s*"[^"]*"'
        replacement = f'"version": "{version}"'
    else:
        pattern = r'^version = "[^"]*"'
        replacement = f'version = "{version}"'
    updated = re.sub(
        pattern,
        replacement,
        content,
        count=1,
        flags=re.MULTILINE,
    )
    if updated == content:
        raise SystemExit(f"failed to update version in {path}")
    if path.as_posix() == "typescript/package.json":
        updated, native_dependency_count = re.subn(
            rf'("{re.escape(NATIVE_PACKAGE_PREFIX)}[^"]+":\s*)"[^"]*"',
            rf'\g<1>"{version}"',
            updated,
        )
        if native_dependency_count == 0:
            raise SystemExit(f"failed to update native dependency versions in {path}")
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
