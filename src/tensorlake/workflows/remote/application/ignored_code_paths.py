import os
from pathlib import Path
from typing import Set


def ignored_code_paths(root_dir: str) -> Set[str]:
    """Returns a set of absolute paths to be ignored when loading or zipping application code."""
    root = Path(root_dir).resolve()
    exclude_paths = set()

    venv_path = os.environ.get("VIRTUAL_ENV")
    if venv_path:
        venv_path = Path(venv_path).resolve()
        try:
            venv_path.relative_to(root)
            exclude_paths.add(str(venv_path))
        except ValueError:
            # venv is not inside root_dir, ignore
            pass

    # 2. Parse .gitignore if present
    gitignore_path = root / ".gitignore"
    if gitignore_path.exists():
        patterns = []
        with gitignore_path.open("r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                patterns.append(line)

        # For each pattern, use glob to find matches
        for pattern in patterns:
            if pattern.endswith("/"):
                pattern = pattern.rstrip("/")
                for match in root.glob(pattern):
                    if match.is_dir():
                        exclude_paths.add(str(match.resolve()))
            else:
                for match in root.glob(pattern):
                    if match.exists():
                        exclude_paths.add(str(match.resolve()))

    return exclude_paths
