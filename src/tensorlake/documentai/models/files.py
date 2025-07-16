from typing import Optional
from pydantic import BaseModel

from tensorlake.documentai.models.enums import MimeType


class FileMetadata(BaseModel):
    """
    Metadata for a file in Document AI.
    """

    file_id: str
    file_name: Optional[str] = None
    mime_type: MimeType
    file_size: int
    checksum_sha256: str
    created_at: str
    labels: Optional[dict] = None
