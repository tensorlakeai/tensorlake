"""
Tensorlake Document AI client
"""

from __future__ import annotations

from typing import Optional

from ._base import _BaseClient
from ._datasets import _DatasetMixin
from ._files import _FilesMixin
from ._parse import _ParseMixin


class DocumentAI(_ParseMixin, _FilesMixin, _DatasetMixin, _BaseClient):
    """
    Document AI client for Tensorlake.
    """

    def __init__(self, api_key: Optional[str] = None, server_url: Optional[str] = None):
        super().__init__(api_key=api_key, server_url=server_url)
