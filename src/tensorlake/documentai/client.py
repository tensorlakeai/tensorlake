"""
Tensorlake Document AI client
"""

from __future__ import annotations

from typing import Optional

from ._base import _BaseClient
from ._classify import _ClassifyMixin
from ._datasets import _DatasetMixin
from ._extract import _ExtractMixin
from ._files import _FilesMixin
from ._parse import _ParseMixin
from ._read import _ReadMixin
from .models import Region


class DocumentAI(
    _ParseMixin,
    _FilesMixin,
    _DatasetMixin,
    _ReadMixin,
    _ExtractMixin,
    _ClassifyMixin,
    _BaseClient,
):
    """
    Document AI client for Tensorlake.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        server_url: Optional[str] = None,
        region: Optional[Region] = Region.US,
    ):
        super().__init__(api_key=api_key, server_url=server_url, region=region)
