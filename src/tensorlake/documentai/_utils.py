from typing import Any, Dict


@staticmethod
def _drop_none(m: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in m.items() if v is not None}
