"""
DocumentAI models package.
"""

# Enums
from .enums import (
    ChunkingStrategy,
    FormDetectionMode,
    JobStatus,
    MimeType,
    ModelProvider,
    ParseStatus,
    TableOutputMode,
    TableParsingFormat,
)

# Job models
from .jobs import (
    Chunk,
    Document,
    Figure,
    Job,
    JobListItem,
    Output,
    Page,
    PageFragment,
    PageFragmentType,
    Signature,
    StructuredData,
    StructuredDataPage,
    Table,
    TableCell,
    Text,
)

# Options
from .options import (
    EnrichmentOptions,
    Options,
    ParsingOptions,
    StructuredExtractionOptions,
)

# Parse models
from .parse import ParseRequest

__all__ = [
    # Enums
    "ChunkingStrategy",
    "FormDetectionMode",
    "JobStatus",
    "MimeType",
    "ModelProvider",
    "ParseStatus",
    "TableOutputMode",
    "TableParsingFormat",
    # Options
    "EnrichmentOptions",
    "Options",
    "ParsingOptions",
    "StructuredExtractionOptions",
    # Parse models
    "ParseRequest",
    # Job models
    "Chunk",
    "Document",
    "Figure",
    "Job",
    "JobListItem",
    "Output",
    "Page",
    "PageFragment",
    "PageFragmentType",
    "Signature",
    "StructuredData",
    "StructuredDataPage",
    "Table",
    "TableCell",
    "Text",
]
