from dataclasses import dataclass


@dataclass(frozen=True)
class Route:
    verb: str
    path: str
