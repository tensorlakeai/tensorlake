from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from typing import Callable, Optional


class KVCache:
    """
    Simple filesystem-backed key/value cache.

    - Values can be stored/retrieved as text or bytes
    - Keys are arbitrary strings; they are hashed to safe filenames
    - Namespacing creates isolated subdirectories under the cache root
    - Default root: ~/.tensorlake/cache
    """

    def __init__(self, namespace: str, root_dir: Optional[Path] = None):
        self.root_dir = root_dir or (Path.home() / ".tensorlake" / "cache")
        self.namespace = namespace
        self.ns_dir = self.root_dir / namespace

    def _key_path(self, key: str, suffix: str = "") -> Path:
        key_str = key if isinstance(key, str) else str(key)
        digest = hashlib.sha256(key_str.encode("utf-8")).hexdigest()
        filename = f"{digest}{suffix}"
        return self.ns_dir / filename

    def get(self, key: str, encoding: str = "utf-8") -> Optional[str]:
        path = self._key_path(key, ".txt")
        if not path.exists():
            return None
        try:
            return path.read_text(encoding=encoding)
        except Exception:
            return None

    def set(self, key: str, value: str, encoding: str = "utf-8") -> None:
        path = self._key_path(key, ".txt")
        try:
            self._write_atomically(
                path, lambda temporary: temporary.write_text(value, encoding=encoding)
            )
        except Exception:
            # Best-effort cache; ignore failures
            pass

    def get_bytes(self, key: str) -> Optional[bytes]:
        path = self._key_path(key, ".bin")
        if not path.exists():
            return None
        try:
            return path.read_bytes()
        except Exception:
            return None

    def set_bytes(self, key: str, data: bytes) -> None:
        path = self._key_path(key, ".bin")
        try:
            self._write_atomically(path, lambda temporary: temporary.write_bytes(data))
        except Exception:
            # Best-effort cache; ignore failures
            pass

    def _write_atomically(self, path: Path, write: Callable[[Path], int]) -> None:
        """Publish a complete value without exposing a partially written entry."""
        self.ns_dir.mkdir(parents=True, exist_ok=True)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=self.ns_dir, prefix=".cache-", delete=False
            ) as temporary:
                temporary_path = Path(temporary.name)
            write(temporary_path)
            os.replace(temporary_path, path)
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def delete(self, key: str) -> None:
        for suffix in (".txt", ".bin"):
            path = self._key_path(key, suffix)
            try:
                if path.exists():
                    path.unlink()
            except Exception:
                pass

    def clear(self) -> None:
        if not self.ns_dir.exists():
            return
        try:
            for p in self.ns_dir.iterdir():
                try:
                    if p.is_file():
                        p.unlink()
                except Exception:
                    continue
        except Exception:
            pass
