from typing import List, Optional, Type, Union

from pydantic import BaseModel, Field, Json

from .enums import (
    ChunkingStrategy,
    FormDetectionMode,
    ModelProvider,
    TableOutputMode,
    TableParsingFormat,
)


class EnrichmentOptions(BaseModel):
    """
    Options for enriching a document with additional information.

    This object helps to extend the output of the document parsing process with additional information.
    This includes summarization of tables and figures, which can help to provide a more comprehensive understanding of the document.
    """

    figure_summarization: bool = Field(
        False,
        description="Boolean flag to enable figure summarization. The default is `false`.",
    )
    figure_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the figure summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `figure_summarization` is set to `true`.",
    )
    table_summarization: bool = Field(
        False,
        description="Boolean flag to enable summary generation for parsed tables. The default is `false`.",
    )
    table_summarization_prompt: Optional[str] = Field(
        None,
        description="The prompt to guide the table summarization. If not provided, a default prompt will be used. It is not required to provide a prompt. The prompt only has effect if `table_summarization` is set to `true`.",
    )


class PageClassificationConfig(BaseModel):
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

    chunking_strategy: Optional[ChunkingStrategy] = None
    disable_layout_detection: Optional[bool] = False
    form_detection_mode: Optional[FormDetectionMode] = (
        FormDetectionMode.OBJECT_DETECTION
    )
    remove_strikethrough: bool = False
    signature_detection: Optional[bool] = False
    skew_detection: bool = False
    table_output_mode: TableOutputMode = TableOutputMode.MARKDOWN
    table_parsing_format: TableParsingFormat = TableParsingFormat.TSR


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
    chunking_strategy: Optional[ChunkingStrategy] = Field(
        None,
        description="The chunking strategy determines how the document is chunked into smaller pieces. This is only supported in `markdown` mode. Not to be confused with the `chunking_strategy` in `DocumentParsingOptions`. which is used to chunk the document into smaller pieces for parsing. The default is `None`, which means no chunking is applied.",
    )
    model_provider: ModelProvider = Field(
        ModelProvider.TENSORLAKE,
        description="The model provider to use for structured data extraction. The default is `tensorlake`, which uses our private model, and runs on our servers.",
    )
    page_classes: Optional[List[str]] = Field(
        None,
        description="The page classes to use for structured data extraction. If not provided, all the pages will be used to extract structured data. The page_classification_config is used to classify the pages of the document.",
    )
    prompt: Optional[str] = Field(
        None,
        description="The prompt to use for structured data extraction. If not provided, the default prompt will be used.",
    )
    skip_ocr: bool = Field(
        False,
        description="Boolean flag to skip converting the document blob to OCR text before structured data extraction. If set to `true`, the API will skip the OCR step and directly extract structured data from the document. The default is `false`.",
    )


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
    page_classifications: Optional[List[PageClassificationConfig]] = Field(
        None,
        description="The properties of this object define the settings for page classification. If this object is present, the API will perform page classification on the document.",
    )
    structured_extraction_options: Optional[List[StructuredExtractionOptions]] = Field(
        None,
        description="The properties of this object define the settings for structured data extraction. If this object is present, the API will perform structured data extraction on the document.",
    )
