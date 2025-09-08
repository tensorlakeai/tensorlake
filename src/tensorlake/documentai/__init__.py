"""
TensorLake Document AI SDK
"""

from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.models import (
    Chunk,
    ChunkingStrategy,
    DatasetDataFilter,
    EnrichmentOptions,
    Figure,
    MimeType,
    ModelProvider,
    OcrPipelineProvider,
    Options,
    Page,
    PageClass,
    PageClassConfig,
    PageFragment,
    PageFragmentType,
    ParseResult,
    ParseStatus,
    ParsingOptions,
    PartitionConfig,
    PartitionStrategy,
    PatternChunking,
    Region,
    Signature,
    StructuredData,
    StructuredExtractionOptions,
    Table,
    TableCell,
    TableOutputMode,
    TableParsingFormat,
    Text,
)

__all__ = [
    "DocumentAI",
    "Region",
    # Enums
    "ChunkingStrategy",
    "MimeType",
    "ModelProvider",
    "OcrPipelineProvider",
    "ParseStatus",
    "PartitionStrategy",
    "TableOutputMode",
    "TableParsingFormat",
    "PatternChunking",
    "PartitionConfig",
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
    # Filters
    "DatasetDataFilter",
]
