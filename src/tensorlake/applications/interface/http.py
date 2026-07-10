from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any


class Headers(Mapping[str, str]):
    """Immutable, case-insensitive HTTP headers mapping."""

    def __init__(
        self,
        headers: Mapping[str, str] | Iterable[tuple[str, str]] | None = None,
    ):
        self._items: tuple[tuple[str, str], ...] = tuple(
            (str(name), str(value))
            for name, value in (
                headers.items() if isinstance(headers, Mapping) else headers or ()
            )
        )
        lookup: dict[str, list[str]] = {}
        for name, value in self._items:
            lookup.setdefault(name.lower(), []).append(value)
        self._lookup: dict[str, tuple[str, ...]] = {
            name: tuple(values) for name, values in lookup.items()
        }

    def __getitem__(self, key: str) -> str:
        values = self._lookup[key.lower()]
        return values[-1]

    def __iter__(self):
        seen: set[str] = set()
        for name, _ in self._items:
            lower_name = name.lower()
            if lower_name in seen:
                continue
            seen.add(lower_name)
            yield name

    def __len__(self) -> int:
        return len(self._lookup)

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and key.lower() in self._lookup

    def get(self, key: str, default: Any = None) -> str | Any:
        values = self._lookup.get(key.lower())
        if values is None:
            return default
        return values[-1]

    def getlist(self, key: str) -> list[str]:
        return list(self._lookup.get(key.lower(), ()))

    def multi_items(self) -> list[tuple[str, str]]:
        return list(self._items)


class HttpBody:
    def __init__(self, content: bytes, content_type: str | None = None):
        self._content: bytes = content
        self._content_type: str | None = content_type

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def content_type(self) -> str | None:
        return self._content_type

    def text(self, encoding: str = "utf-8") -> str:
        return self._content.decode(encoding)

    def json(self) -> Any:
        return json.loads(self.text())

    def __str__(self) -> str:
        return (
            f"HttpBody(content_type={self._content_type}, "
            f"content={self._content[:20]}...)"
        )
