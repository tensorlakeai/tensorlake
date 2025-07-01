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

    chunking_strategy: Optional[ChunkingStrategy] = None
    json_schema: Union[Type[BaseModel], Json] = Field(..., alias="schema")
    model_provider: ModelProvider = ModelProvider.TENSORLAKE
    page_class: Optional[str] = None
    page_class_definition: Optional[str] = None
    prompt: Optional[str] = None
    schema_name: str
    skip_ocr: bool = False

    class Config:
        validate_by_name = True  # Enables usage of 'schema=' as well


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
