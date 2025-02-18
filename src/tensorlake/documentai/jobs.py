"""
DocumentAI job classes.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """
    Status of a job.
    """

    PROCESSING = "processing"
    SUCCESSFUL = "successful"
    FAILURE = "failure"
    PENDING = "pending"

class JobItem(BaseModel):
    """
    DocumentAI job item class.

    """
    id: str
    file_id: str = Field(alias="fileId")
    status: JobStatus
    job_type: str = Field(alias="jobType")
    error_message: Optional[str] = Field(alias="errorMessage")
    created_at: str = Field(alias="createdAt")
    updated_at: str = Field(alias="updatedAt")
