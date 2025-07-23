from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from ._enums import ParseStatus
from ._pagination import PaginationDirection


@dataclass(slots=True)
class DatasetDataFilter:
    """
    Filter for dataset data retrieval.

    Attributes:
      cursor: Optional cursor for pagination. If provided, the method will return the next page of
          results starting from this cursor. If not provided, it will return the first page of results.

      direction: Optional pagination direction. If provided, it can be "next" or "prev" to navigate through the pages.

      limit: Optional limit on the number of results to return. If not provided, a default limit will be used.

      file_name: Optional filename to filter the results by. If provided, only parse results associated with this filename will be returned.

      status: Optional status to filter the results by. If provided, only parse results with this status will be returned.

      created_after: Optional timestamp to filter the results by creation time. If provided, only parse results created after this timestamp will be
          returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

      created_before: Optional timestamp to filter the results by creation time. If provided, only parse results created before this timestamp will be
          returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

      finished_after: Optional timestamp to filter the results by finish time. If provided, only parse results finished after this timestamp will be
          returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").

      finished_before: Optional timestamp to filter the results by finish time. If provided, only parse results finished before this timestamp will be
          returned. The date should be in RFC3339 format (e.g., "2023-10-01T00:00:00Z").
    """

    cursor: Optional[str] = None
    direction: Optional[PaginationDirection] = None
    limit: Optional[int] = None
    file_name: Optional[str] = None
    status: Optional[ParseStatus] = None
    created_after: Optional[str] = None
    created_before: Optional[str] = None
    finished_after: Optional[str] = None
    finished_before: Optional[str] = None

    def to_query_params(self) -> Dict[str, Any]:
        """
        Convert the filter to a dictionary of query parameters.

        This method converts the filter's attributes into a dictionary suitable for use as query parameters.
        It omits any attributes that are `None` and converts enum values to their string representations
        """
        raw = asdict(self)
        # enum → value
        if raw["direction"] is not None:
            raw["direction"] = raw["direction"].value
        if raw["status"] is not None:
            raw["status"] = raw["status"].value
        return {k: v for k, v in raw.items() if v is not None}
