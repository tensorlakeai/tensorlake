from typing import List, Optional, Set, Type, Union

from pydantic import BaseModel, Field, Json, field_serializer, field_validator

from ._enums import (
    ChunkingStrategy,
    ModelProvider,
    OcrPipelineProvider,
    PageFragmentType,
    PartitionConfig,
    PartitionStrategy,
    PatternConfig,
    PatternPartitionStrategy,
    SimplePartitionStrategy,
    TableOutputMode,
    TableParsingFormat,
)


class EnrichmentOptions(BaseModel):
    """
    Options for enriching a document with additional information.

    This object helps to extend the output of the document parsing process with additional information.
    This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document.
    """

    figure_summarization: Optional[bool] = Field(
        None,
        description="Boolean flag to enable figure summarization. The default is `false`.",
    )
    figure_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the figure summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `figure_summarization` is set to `true`.",
    )
    table_summarization: Optional[bool] = Field(
        None,
        description="Boolean flag to enable summary generation for parsed tables. The default is `false`.",
    )
    table_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the table summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `table_summarization` is set to `true`.",
    )
    include_full_page_image: Optional[bool] = Field(
        None,
        description="Use full page image in addition to the cropped table and figure images. This provides Language Models context about the table and figure they are summarizing in addition to the cropped images, and could improve the summarization quality. The default is `false`.",
    )


class PageClassConfig(BaseModel):
    """
    Configuration for page classification.
    """

    name: str = Field(description="The name of the page class.")
    description: str = Field(
        description="The description of the page class to guide the model to classify the pages. Describe what the model should look for in the page to classify it."
    )


class ParsingOptions(BaseModel):
    """
    Options for parsing a document.
    """

    chunking_strategy: Optional[ChunkingStrategy] = Field(
        None,
        description="The chunking strategy determines how the document is chunked into smaller pieces. Different strategies can be used to optimize the parsing process. Choose the one that best fits your use case. The default is `None`, which means no chunking is applied.",
    )
    cross_page_header_detection: Optional[bool] = Field(
        None,
        description="Flag to enable header-hierarchy detection across pages. When set to `true`, the parser will consider headers from different pages when determining the hierarchy of headers within a single page.",
    )
    disable_layout_detection: Optional[bool] = Field(
        None,
        description="Useful flag for documents with a lot of tables or images. If set to `true`, the API will skip the layout detection step, and directly extract text from the document.",
    )
    ocr_model: Optional[OcrPipelineProvider] = Field(
        None,
        description="The model to use for OCR (Optical Character Recognition).",
    )
    remove_strikethrough_lines: Optional[bool] = Field(
        None,
        description="Flag to enable the detection, and removal, of strikethrough text in the document. This flag incurs additional billing costs. The default is `false`.",
    )
    signature_detection: Optional[bool] = Field(
        None,
        description="Flag to enable the detection of signatures in the document. This flag incurs additional billing costs. The default is `false`.",
    )
    skew_detection: Optional[bool] = Field(
        None,
        description="Boolean flag to detect and correct skewed or rotated pages in the document. The default is `false`. Setting this to `true` will increase the processing time of the document.",
    )
    table_output_mode: Optional[TableOutputMode] = Field(
        None,
        description="The format for the tables extracted from the document. This options determines how the tables are represented in the json response. The default is `HTML`, which means the tables are represented as HTML strings.",
    )
    table_parsing_format: Optional[TableParsingFormat] = Field(
        None,
        description="Determines how the system identifies and extracts tables from the document. Default is `table_structure_recognition`, which is better suited for clean, grid-like tables.",
    )
    ignore_sections: Optional[Set[PageFragmentType]] = Field(
        None,
        description="Set of page fragment types to ignore during parsing. This can be used to skip certain types of content, such as headers, footers, or other non-essential elements. If not provided, all page fragment types will be considered.",
    )

    @field_serializer("ignore_sections")
    def serialize_ignore_sections(
        self, ignore_sections: Set[PageFragmentType]
    ) -> List[str]:
        return [section.value for section in ignore_sections]


class StructuredExtractionOptions(BaseModel):
    """
    Options for structured data extraction from a document.
    """

    # Required fields
    schema_name: str = Field(
        description="The name of the schema. This is used to tag the structured data output with a name in the response."
    )
    json_schema: Union[Type[BaseModel], Json, dict] = Field(
        description="The JSON schema to guide structured data extraction from the file. This schema should be a valid JSON schema that defines the structure of the data to be extracted. The API supports a subset of the JSON schema specification. This value must be provided if `structured_extraction` is present in the request."
    )

    # Optional fields
    partition_strategy: Optional[PartitionConfig] = Field(
        default=None,
        description="Strategy to partition the document before structured data extraction. The API will return one structured data object per partition. This is useful when you want to extract certain fields from every page.",
    )
    model_provider: Optional[ModelProvider] = Field(
        default=None,
        description="The model provider to use for structured data extraction. The default is `tensorlake`, which uses our private model, and runs on our servers.",
    )
    page_classes: Optional[List[str]] = Field(
        default=None,
        description="The page classes to use for structured data extraction. If not provided, all the pages will be used to extract structured data. The page_classification_config is used to classify the pages of the document.",
    )
    prompt: Optional[str] = Field(
        default=None,
        description="The prompt to use for structured data extraction. If not provided, the default prompt will be used.",
    )
    provide_citations: Optional[bool] = Field(
        default=None,
        description="Flag to enable visual citations in the structured data output. It returns the bounding boxes of the coordinates of the document where the structured data was extracted from.",
    )
    skip_ocr: Optional[bool] = Field(
        default=None,
        description="Boolean flag to skip converting the document blob to OCR text before structured data extraction. If set to `true`, the API will skip the OCR step and directly extract structured data from the document. The default is `false`.",
    )

    @field_validator("partition_strategy", mode="before")
    @classmethod
    def _normalize_partition_strategy(cls, v):
        if v is None:
            return v

        if isinstance(v, PartitionStrategy):
            return SimplePartitionStrategy(strategy=v.value)

        # Handle string values
        if isinstance(v, str):
            if v == "patterns":
                raise ValueError(
                    "Cannot use 'patterns' strategy without pattern configuration"
                )
            return SimplePartitionStrategy(strategy=v)

        if isinstance(v, dict):
            if "patterns" in v:
                return PatternPartitionStrategy(patterns=PatternConfig(**v["patterns"]))

            elif "strategy" in v and v["strategy"] == "patterns":
                return PatternPartitionStrategy(
                    patterns=PatternConfig(
                        start_patterns=v.get("start_patterns"),
                        end_patterns=v.get("end_patterns"),
                    )
                )
            else:
                return SimplePartitionStrategy(strategy=v["strategy"])

        if isinstance(v, (SimplePartitionStrategy, PatternPartitionStrategy)):
            return v

        return v

    @field_serializer("partition_strategy")
    def _serialize_partition_strategy(self, v):
        if v is None:
            return None
        if isinstance(v, SimplePartitionStrategy):
            return v.strategy
        if isinstance(v, PatternPartitionStrategy):
            return {
                "patterns": {
                    "start_patterns": v.patterns.start_patterns,
                    "end_patterns": v.patterns.end_patterns,
                }
            }
        return v

    @field_validator("partition_strategy", mode="after")
    @classmethod
    def _validate_partition_strategy_type(cls, v):
        if v is None:
            return v

        # Custom validation since we can't use automatic discriminator
        if isinstance(v, dict):
            if "patterns" in v:
                return PatternPartitionStrategy(patterns=PatternConfig(**v["patterns"]))

            return SimplePartitionStrategy(strategy=v["strategy"])

        return v


class Options(BaseModel):
    """
    Options for configuring document parsing operations.
    """

    enrichment_options: Optional[EnrichmentOptions] = Field(
        None,
        description="The properties of this object help to extend the output of the document parsing process with additional information. This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document. This object is not required, and the API will use default settings if it is not present.",
    )
    parsing_options: Optional[ParsingOptions] = Field(
        None,
        description="Additional options for tailoring the document parsing process. This object allows you to customize how the document is parsed, including table parsing, chunking strategies, and more. It is not required to provide this object, and the API will use default settings if it is not present.",
    )
    page_classifications: Optional[List[PageClassConfig]] = Field(
        None,
        description="The properties of this object define the settings for page classification. If this object is present, the API will perform page classification on the document.",
    )
    structured_extraction_options: Optional[List[StructuredExtractionOptions]] = Field(
        None,
        description="The properties of this object define the settings for structured data extraction. If this object is present, the API will perform structured data extraction on the document.",
    )
