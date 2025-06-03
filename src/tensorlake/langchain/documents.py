import time
import os
from typing import Optional, Type, Union
from langchain_core.tools import StructuredTool
from pydantic import Field, BaseModel, Json

from tensorlake.documentai import DocumentAI, ParsingOptions
from tensorlake.documentai.parse import (
    ChunkingStrategy,
    TableParsingStrategy,
    TableOutputMode,
    ExtractionOptions,
    ModelProvider,
    FormDetectionMode
)
load_dotenv()
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")


class DocumentParserOptions(BaseModel):
    """Comprehensive options for parsing a document with Tensorlake."""

    # Chunking options
    chunking_strategy: Optional[ChunkingStrategy] = Field(
        default=ChunkingStrategy.PAGE,
        description="Strategy for chunking the document (NONE, PAGE, or SECTION_HEADER)"
    )

    # Table parsing options
    table_parsing_strategy: TableParsingStrategy = Field(
        default=TableParsingStrategy.VLM,
        description="Algorithm for parsing tables (TSR for structured tables, VLM for complex/unstructured tables)"
    )
    table_output_mode: TableOutputMode = Field(
        default=TableOutputMode.MARKDOWN,
        description="Format for table output (JSON, MARKDOWN, or HTML)"
    )
    table_parsing_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt to guide table parsing"
    )
    table_summary: bool = Field(
        default=False,
        description="Whether to generate summaries of tables"
    )

    # Figure and image options
    figure_summary: bool = Field(
        default=False,
        description="Whether to generate summaries of figures and images"
    )
    figure_summarization_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt for figure summarization"
    )

    # Page and layout options
    page_range: Optional[str] = Field(
        default=None,
        description="Specific page range to parse (e.g., '1-5' or '1,3,5')"
    )
    skew_correction: bool = Field(
        default=False,
        description="Whether to apply skew correction to scanned documents"
    )
    disable_layout_detection: bool = Field(
        default=False,
        description="Whether to disable automatic layout detection"
    )

    # Signature and form detection
    detect_signature: bool = Field(
        default=False,
        description="Whether to detect the presence of signatures in the document"
    )
    form_detection_mode: FormDetectionMode = Field(
        default=FormDetectionMode.OBJECT_DETECTION,
        description="Algorithm for form detection (VLM or OBJECT_DETECTION)"
    )

    # Structured extraction options
    extraction_schema: Optional[Union[Type[BaseModel], Json]] = Field(
        default=None,
        description="JSON schema for structured data extraction"
    )
    extraction_prompt: Optional[str] = Field(
        default=None,
        description="Custom prompt for structured data extraction"
    )
    extraction_model_provider: ModelProvider = Field(
        default=ModelProvider.TENSORLAKE,
        description="Model provider for extraction (TENSORLAKE, SONNET, or GPT4OMINI)"
    )
    skip_ocr: bool = Field(
        default=False,
        description="Whether to skip OCR processing for text-based PDFs"
    )

    # Webhook options
    deliver_webhook: bool = Field(
        default=False,
        description="Whether to deliver results via webhook when processing is complete"
    )

    # Processing timeout
    timeout_seconds: int = Field(
        default=300,
        description="Maximum time to wait for processing completion (in seconds)"
    )


def document_to_markdown_converter(path: str, options: DocumentParserOptions) -> str:
    """
    Convert a document to markdown using Tensorlake's DocumentAI.

    Args:
        path: Path to the document file to parse (supports PDF, DOCX, images, etc.)
        options: DocumentParserOptions object containing all parsing configuration

    Returns:
        str: The parsed document in markdown format, or error message if failed

    Raises:
        ValueError: If API key is not configured
        Exception: If document processing fails
    """
    if not TENSORLAKE_API_KEY:
        return "Error: TENSORLAKE_API_KEY environment variable is not set"

    try:
        # Initialize DocumentAI client
        doc_ai = DocumentAI(api_key=TENSORLAKE_API_KEY)

        # Upload document to TensorLake
        file_id = doc_ai.upload(path=path)

        # Configure parsing options based on user input
        parsing_options = ParsingOptions(
            chunking_strategy=options.chunking_strategy,
            table_parsing_strategy=options.table_parsing_strategy,
            table_output_mode=options.table_output_mode,
            table_parsing_prompt=options.table_parsing_prompt,
            table_summary=options.table_summary,
            figure_summary=options.figure_summary,
            figure_summarization_prompt=options.figure_summarization_prompt,
            page_range=options.page_range,
            skew_correction=options.skew_correction,
            disable_layout_detection=options.disable_layout_detection,
            detect_signature=options.detect_signature,
            form_detection_mode=options.form_detection_mode,
            deliver_webhook=options.deliver_webhook
        )

        # Add extraction options if schema is provided
        schema = options.extraction_schema
        if isinstance(schema, dict):
            import json
            schema = json.dumps(schema)
        if schema:
            parsing_options.extraction_options = ExtractionOptions(
                schema=schema,
                prompt=options.extraction_prompt,
                provider=options.extraction_model_provider,
                skip_ocr=options.skip_ocr
            )
        elif options.skip_ocr:
            parsing_options.extraction_options = ExtractionOptions(
                provider=options.extraction_model_provider,
                skip_ocr=options.skip_ocr
            )

        # Start parsing job
        job_id = doc_ai.parse(file_id, options=parsing_options)

        # Poll for completion with configurable timeout
        start_time = time.time()
        max_wait_time = options.timeout_seconds

        while time.time() - start_time < max_wait_time:
            result = doc_ai.get_job(job_id)

            if result.status in ["pending", "processing"]:
                time.sleep(5)  # Wait 5 seconds before checking again
            elif result.status == "successful":
                # Return the parsed content
                if hasattr(result, 'content') and result.content:
                    return result.content
                elif hasattr(result, 'markdown') and result.markdown:
                    return result.markdown
                else:
                    return str(result)  # Fallback to string representation
            else:
                return f"Document parsing failed with status: {result.status}"

        # Timeout reached
        return f"Document processing timeout after {max_wait_time} seconds. Job ID: {job_id}"

    except Exception as e:
        return f"Error processing document: {str(e)}"


async def document_to_markdown_converter_async(path: str, options: DocumentParserOptions) -> str:
    """Asynchronous version of document to markdown converter."""
    import asyncio
    return await asyncio.to_thread(document_to_markdown_converter, path, options)


# Create the LangChain tool using StructuredTool
document_to_markdown_tool = StructuredTool.from_function(
    func=document_to_markdown_converter,
    coroutine=document_to_markdown_converter_async,
    name="DocumentToMarkdownConverter",
    description="Convert documents (PDF, DOCX, images, etc.) to markdown using Tensorlake AI. Supports tables, figures, signatures, and structured extraction.",
    return_direct=False,
    handle_tool_error="Document parsing failed. Please verify the file path and your Tensorlake API key."
)
