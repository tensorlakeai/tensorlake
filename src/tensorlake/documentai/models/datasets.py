from typing import Optional

from pydantic import BaseModel

from .enums import DatasetStatus


class Dataset(BaseModel):
    name: str
    slug: str
    status: DatasetStatus
    description: Optional[str] = None
    created_at: str


class DatasetParseRequest(BaseModel):
    dataset_id: str
    file_id: Optional[str] = None
    file_url: Optional[str] = None
    raw_text: Optional[str] = None
    labels: Optional[dict] = None
    mime_type: Optional[str] = None
    page_range: Optional[str] = None

    class Config:
        schema_extra = {
            "example": {
                "dataset_id": "dataset-123",
                "file_id": "tensorlake-file-456",
                "file_url": "https://example.com/document.pdf",
                "raw_text": "This is a sample text.",
                "labels": {"key1": "value1", "key2": "value2"},
                "mime_type": "application/pdf",
                "page_range": "1,2,3-5",
            }
        }
