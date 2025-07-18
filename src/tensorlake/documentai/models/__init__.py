"""
DocumentAI models package.
"""

from ._datasets import (
    Dataset,
    DatasetStatus,
)

# Enums
from ._enums import (
    ChunkingStrategy,
    MimeType,
    ModelProvider,
    ParseStatus,
    PartitionStrategy,
    TableOutputMode,
    TableParsingFormat,
)

# Options
from ._options import (
    EnrichmentOptions,
    Options,
    PageClassConfig,
    ParsingOptions,
    StructuredExtractionOptions,
)
from ._pagination import PaginatedResult, PaginationDirection

# Results models
from ._results import (
    Chunk,
    Figure,
    Page,
    PageClass,
    PageFragment,
    PageFragmentType,
    ParseRequestOptions,
    ParseResult,
    Signature,
    StructuredData,
    Table,
    TableCell,
    Text,
)

__all__ = [
    # Enums
    "ChunkingStrategy",
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
    # Pagination
    "PaginatedResult",
    "PaginationDirection",
]
