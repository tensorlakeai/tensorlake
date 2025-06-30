"""
TensorLake Document AI SDK
"""

from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.datasets import DatasetOptions, IngestArgs
from tensorlake.documentai.jobs import Job, JobStatus, Output
from tensorlake.documentai.parse import (
    ChunkingStrategy,
    EnrichmentOptions,
    FormDetectionMode,
    MimeType,
    ModelProvider,
    ParseRequest,
    ParsingOptions,
    StructuredExtractionOptions,
    TableOutputMode,
    TableParsingFormat,
)

__all__ = [
    "DocumentAI",
    "DatasetOptions",
    "IngestArgs",
    "Job",
    "JobStatus",
    "Output",
    # Parse-related classes and enums
    "ChunkingStrategy",
    "EnrichmentOptions",
    "FormDetectionMode",
    "MimeType",
    "ModelProvider",
    "ParseRequest",
    "ParsingOptions",
    "StructuredExtractionOptions",
    "TableOutputMode",
    "TableParsingFormat",
]
