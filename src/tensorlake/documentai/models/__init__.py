"""
DocumentAI models package.
"""

from ._datasets import Dataset, DatasetStatus

# Enums
from ._enums import (
    ChunkingStrategy,
    MimeType,
    ModelProvider,
    OcrPipelineProvider,
    ParseStatus,
    PartitionConfig,
    PartitionStrategy,
    PatternConfig,
    PatternPartitionStrategy,
    Region,
    SimplePartitionStrategy,
    TableOutputMode,
    TableParsingFormat,
)
from ._errors import DocumentAIError, ErrorCode, ErrorResponse
from ._filters import DatasetDataFilter

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
    "OcrPipelineProvider",
    "ParseStatus",
    "PartitionStrategy",
    "TableOutputMode",
    "TableParsingFormat",
    "PatternPartitionStrategy",
    "PartitionConfig",
    "SimplePartitionStrategy",
    "PatternConfig",
    # Options
    "EnrichmentOptions",
    "Options",
    "PageClassConfig",
    "ParsingOptions",
    "StructuredExtractionOptions",
    # Results models
    "PageClass",
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
    # Filters
    "DatasetDataFilter",
    "Region",
    # Errors
    "ErrorCode",
    "ErrorResponse",
    "DocumentAIError",
]
