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


def _print_info(message: str):
    print(f"{CYAN}{message}{RESET}")


def _print_warn(message: str):
    print(f"{YELLOW}{message}{RESET}")


def _print_success(message: str):
    print(f"{GREEN}{message}{RESET}")


def _print_error(message: str):
    print(f"{RED}{message}{RESET}")


def _print_update(message: str):
    print(f"{BLUE}{message}{RESET}")


def _print_magenta(message: str):
    print(f"{MAGENTA}{message}{RESET}")


def _print_bold(message: str):
    print(f"{BOLD}{message}{RESET}")


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


def get_doc_ai_base_url_v1(region: Region, server_url: Optional[str] = None) -> str:
    """
    Returns the base URL for the Document AI API based on the region.

    If server_url is provided, it will be used as the base URL.
    Otherwise, it will fall back to the environment variable TENSORLAKE_DOCAI_URL
    or the default server URL based on the region.
    """
    if server_url:
        return f"{server_url}/documents/v1/"

    v1_url = os.getenv("TENSORLAKE_DOCAI_URL")
    if v1_url:
        return v1_url

    return f"{get_server_url(region)}/documents/v1/"


def get_doc_ai_base_url_v2(region: Region, server_url: Optional[str] = None) -> str:
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
