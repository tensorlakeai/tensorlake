"""
DocumentAI models package.
"""

from .datasets import Dataset

# Enums
from .enums import (
    ChunkingStrategy,
    DatasetStatus,
    JobStatus,
    MimeType,
    ModelProvider,
    ParseStatus,
    PartitionStrategy,
    TableOutputMode,
    TableParsingFormat,
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
from .results import (
    PageClass,
    ParseRequestOptions,
    ParseResult,
    Chunk,
    Figure,
    Page,
    PageFragment,
    PageFragmentType,
    Signature,
    StructuredData,
    Table,
    TableCell,
    Text,
)

__all__ = [
    # Enums
    "ChunkingStrategy",
    "JobStatus",
    "MimeType",
    "ModelProvider",
    "ParseStatus",
    "PartitionStrategy",
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
    "Chunk",
    "Figure",
    "Page",
    "PageFragment",
    "PageFragmentType",
    "Signature",
    "StructuredData",
    "Table",
    "TableCell",
    "Text",
    # Datasets
    "Dataset",
    "DatasetStatus",
]
