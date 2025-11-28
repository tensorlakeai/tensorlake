"""
Common types and constants for the Document AI API.
"""

import os
from typing import Generic, List, Optional, TypeVar

from pydantic import BaseModel, Field

from .models import Region

RESET = "\033[0m"
BOLD = "\033[1m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
MAGENTA = "\033[35m"
GREEN = "\033[32m"
RED = "\033[31m"


def get_server_url(region: Region) -> str:
    """
    Returns the base URL for the Document AI API.
    """

    tensorlake_api_url = os.getenv("TENSORLAKE_API_URL")
    if tensorlake_api_url:
        return tensorlake_api_url

    if region == Region.EU:
        return "https://api.eu.tensorlake.ai"

    return "https://api.tensorlake.ai"


def get_doc_ai_base_url(region: Region, server_url: Optional[str] = None) -> str:
    """
    Returns the base URL for the Document AI API v2 based on the region.
    """
    if server_url:
        return f"{server_url}/documents/v2/"

    v2_url = os.getenv("TENSORLAKE_DOCAI_URL_V2")
    if v2_url:
        return v2_url

    return f"{get_server_url(region)}/documents/v2/"


T = TypeVar("T")


class PaginatedResult(BaseModel, Generic[T]):
    """
    A slice from a paginated endpoint.
    """

    items: List[T] = Field(alias="items")
    has_more: bool = Field(alias="hasMore")
    prev_cursor: Optional[str] = Field(alias="prevCursor")
    next_cursor: Optional[str] = Field(alias="nextCursor")
