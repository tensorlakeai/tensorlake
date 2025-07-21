from enum import Enum
from typing import Optional

from pydantic import BaseModel


class DatasetStatus(str, Enum):
    """
    Enum representing the status of a dataset.

    Attributes:
        IDLE: The dataset is idle and ready for use.
        PROCESSING: At least one parse operation is currently in pending or processing state.
    """

    IDLE = "idle"
    PROCESSING = "processing"


class Dataset(BaseModel):
    """
    Represents a dataset in the Tensorlake Cloud project.

    Attributes:
        name (str): The name of the dataset.
        dataset_id (str): The unique identifier for the dataset. Used to reference the dataset in API calls.
        status (DatasetStatus): The current status of the dataset. May be one of the following:
            - `idle`: The dataset is idle and ready for use.
            - `processing`: The dataset is currently being processed.
        description (Optional[str]): A description given to the dataset. This is optional and can be used to provide additional context or information about the dataset.
        created_at (str): The timestamp when the dataset was created. Formatted as a RFC 3339 string (e.g., "2023-10-01T12:00:00Z").
    """

    name: str
    dataset_id: str
    status: DatasetStatus
    description: Optional[str] = None
    created_at: str
