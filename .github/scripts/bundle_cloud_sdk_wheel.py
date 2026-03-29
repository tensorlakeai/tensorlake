from __future__ import annotations

import argparse
import zipfile
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract the bundled _cloud_sdk native module from a built wheel."
    )
    parser.add_argument(
        "wheel_glob",
        help="Glob that resolves to exactly one cloud SDK wheel",
    )
    parser.add_argument(
        "output_dir",
        help="Directory where the extracted extension should be written",
    )
    parser.add_argument(
        "--require-abi3",
        action="store_true",
        help="Fail unless the input wheel filename contains abi3",
    )
    args = parser.parse_args()

    matches = sorted(Path().glob(args.wheel_glob))
    if len(matches) != 1:
        raise SystemExit(
            f"Expected exactly one wheel matching {args.wheel_glob!r}, found {len(matches)}: {matches}"
        )

    wheel = matches[0].resolve()
    if args.require_abi3 and "abi3" not in wheel.name:
        raise SystemExit(
            f"Cloud SDK wheel must be abi3 for Python 3.10+ compatibility, got: {wheel.name}"
        )

    out_dir = Path(args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    for pattern in ("_cloud_sdk*.so", "_cloud_sdk*.pyd"):
        for path in out_dir.glob(pattern):
            path.unlink()

    with zipfile.ZipFile(wheel) as zf:
        members = [
            member
            for member in zf.namelist()
            if Path(member).name.startswith("_cloud_sdk")
            and (member.endswith(".so") or member.endswith(".pyd"))
        ]
        if len(members) != 1:
            raise SystemExit(
                f"Expected exactly one _cloud_sdk extension in {wheel}, got {members}"
            )

        member = members[0]
        target = out_dir / Path(member).name
        target.write_bytes(zf.read(member))
        print(f"Bundled {member} -> {target}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
