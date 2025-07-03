"""
DocumentAI models package.
"""

# Enums
from .enums import (
    ChunkingStrategy,
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
    PageClassConfig,
    ParsingOptions,
    StructuredExtractionOptions,
)

# Parse models
from .parse import ParseRequest

# Results models
from .results import PageClass, ParseRequestOptions, ParseResult

__all__ = [
    # Enums
    "ChunkingStrategy",
    "JobStatus",
    "MimeType",
    "ModelProvider",
    "ParseStatus",
    "TableOutputMode",
    "TableParsingFormat",
    # Options
    "EnrichmentOptions",
    "Options",
    "PageClassConfig",
    "ParsingOptions",
    "StructuredExtractionOptions",
    # Parse models
    "ParseRequest",
    # Results models
    "PageClass",
    "ParseRequestOptions",
    "ParseResult",
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
