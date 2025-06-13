import time
import os
from typing import Any, List, Optional, Union
from agno.tools import Toolkit
from agno.utils.log import logger

try:
    from tensorlake.documentai import DocumentAI, ParsingOptions
    from tensorlake.documentai.parse import (
        ChunkingStrategy,
        TableParsingStrategy,
        TableOutputMode,
        ExtractionOptions,
        ModelProvider,
        FormDetectionMode
    )
except ImportError:
    raise ImportError("`tensorlake` not installed. Please install using `pip install tensorlake`")


class TensorLakeTools(Toolkit):
    """
    TensorLake DocumentAI toolkit for advanced document parsing and analysis.

    This toolkit provides AI-powered document processing capabilities including:
    - Converting documents to markdown
    - Table extraction and analysis
    - Figure and image summarization
    - Signature detection
    - Structured data extraction
    - Form field detection

    Args:
        api_key: TensorLake API key (can also be set via TENSORLAKE_API_KEY env var)
        document_path: Document path which needs to be parsed
        parse_to_markdown: Enable document to markdown conversion
        extract_structured_data: Enable structured data extraction
        timeout_seconds: Default timeout for processing jobs
        detect_signature: Default setting for signature detection
        skip_ocr: Default setting for OCR (set True for digital PDFs to preserve quality)
        skew_correction: Default setting for skew correction (set True for scanned documents)
    """

    def __init__(
            self,
            api_key: Optional[str] = None,
            document_path: Optional[str] = None,
            parse_to_markdown: bool = True,
            extract_structured_data: bool = True,
            timeout_seconds: int = 300,
            detect_signature: bool = False,
            skip_ocr: bool = False,
            skew_correction: bool = False,
            **kwargs,
    ):
        # Get API key from environment if not provided
        self.api_key = api_key or os.getenv("TENSORLAKE_API_KEY")

        if not self.api_key:
            logger.error("TENSORLAKE_API_KEY not set. Please set the TENSORLAKE_API_KEY environment variable.")

        self.document_path = document_path
        self.timeout_seconds = timeout_seconds
        self.default_detect_signature = detect_signature
        self.default_skip_ocr = skip_ocr
        self.default_skew_correction = skew_correction

        tools: List[Any] = []
        if parse_to_markdown:
            tools.append(self.parse_document_to_markdown)
        if extract_structured_data:
            tools.append(self.extract_structured_data_from_document)

        super().__init__(name="tensorlake", tools=tools, **kwargs)

    def parse_document_to_markdown(
            self,
            document_path: Optional[str] = None,
            detect_signature: Optional[bool] = None,
            skip_ocr: Optional[bool] = None,
            skew_correction: Optional[bool] = None,
            table_output_mode: str = "markdown",
            chunking_strategy: str = "page",
            table_parsing_strategy: str = "vlm",
            page_range: Optional[str] = None,
            figure_summary: bool = False,
            table_summary: bool = False,
            table_parsing_prompt: Optional[str] = None,
            figure_summarization_prompt: Optional[str] = None,
    ) -> str:
        """Parse a document and convert it to markdown format with AI-powered analysis.

        This function can handle various document formats (PDF, DOCX, images) and provides
        advanced parsing capabilities including table extraction, figure analysis, and more.

        Args:
            document_path: Path to the document file to parse. If not provided, uses document_path from toolkit initialization
            detect_signature: Whether to detect signatures in the document
            skip_ocr: Skip OCR for text-based PDFs to preserve quality and speed up processing
            skew_correction: Apply skew correction for scanned/photographed documents
            table_output_mode: Format for table output - "markdown", "json", or "html"
            chunking_strategy: How to break down the document - "none", "page", "section", or "fragment"
            table_parsing_strategy: Algorithm for parsing tables - "tsr" for clean tables, "vlm" for complex ones
            page_range: Specific pages to parse (e.g., '1-5', '1,3,5', '10-end')
            figure_summary: Whether to generate summaries of figures and charts
            table_summary: Whether to generate summaries of tables
            table_parsing_prompt: Custom prompt to guide table parsing
            figure_summarization_prompt: Custom prompt for figure analysis

        Returns:
            str: The parsed document in markdown format, or error message if failed
        """
        if not self.api_key:
            return "Error: TENSORLAKE_API_KEY is not configured"

        # Use provided path or fall back to default
        doc_path = document_path or self.document_path
        if not doc_path:
            return "Error: No document path provided and no default path set"

        # Use provided values or fall back to defaults
        detect_sig = detect_signature if detect_signature is not None else self.default_detect_signature
        skip_ocr_val = skip_ocr if skip_ocr is not None else self.default_skip_ocr
        skew_corr = skew_correction if skew_correction is not None else self.default_skew_correction

        try:
            # Initialize DocumentAI client
            doc_ai = DocumentAI(api_key=self.api_key)

            # Upload document
            logger.info(f"Uploading document: {doc_path}")
            file_id = doc_ai.upload(path=doc_path)

            # Configure parsing options
            parsing_options = ParsingOptions()

            # Set important flags first
            parsing_options.detect_signature = detect_sig
            parsing_options.skew_correction = skew_corr

            # Set table options
            if table_output_mode.lower() == "json":
                parsing_options.table_output_mode = TableOutputMode.JSON
            elif table_output_mode.lower() == "html":
                parsing_options.table_output_mode = TableOutputMode.HTML
            else:
                parsing_options.table_output_mode = TableOutputMode.MARKDOWN

            # Set chunking strategy
            if chunking_strategy.lower() == "none":
                parsing_options.chunking_strategy = ChunkingStrategy.NONE
            elif chunking_strategy.lower() == "section":
                parsing_options.chunking_strategy = ChunkingStrategy.SECTION_HEADER
            elif chunking_strategy.lower() == "fragment":
                parsing_options.chunking_strategy = ChunkingStrategy.FRAGMENT
            else:
                parsing_options.chunking_strategy = ChunkingStrategy.PAGE

            # Set table parsing strategy
            if table_parsing_strategy.lower() == "tsr":
                parsing_options.table_parsing_strategy = TableParsingStrategy.TSR
            else:
                parsing_options.table_parsing_strategy = TableParsingStrategy.VLM

            # Set other options
            if page_range:
                parsing_options.page_range = page_range

            parsing_options.figure_summary = figure_summary
            parsing_options.table_summary = table_summary

            if table_parsing_prompt:
                parsing_options.table_parsing_prompt = table_parsing_prompt

            if figure_summarization_prompt:
                parsing_options.figure_summarization_prompt = figure_summarization_prompt

            # Set extraction options if needed
            if skip_ocr_val:
                parsing_options.extraction_options = ExtractionOptions(skip_ocr=skip_ocr_val)

            # Start parsing job
            logger.info("Starting document parsing job")
            job_id = doc_ai.parse(file_id, options=parsing_options)

            # Poll for completion
            start_time = time.time()
            while time.time() - start_time < self.timeout_seconds:
                result = doc_ai.get_job(job_id)

                if result.status in ["pending", "processing"]:
                    logger.info(f"Job {job_id} is {result.status}, waiting...")
                    time.sleep(5)
                elif result.status == "successful":
                    logger.info("Document parsing completed successfully")
                    # Return the parsed content
                    if hasattr(result, 'content') and result.content:
                        return result.content
                    elif hasattr(result, 'markdown') and result.markdown:
                        return result.markdown
                    else:
                        return str(result)
                else:
                    return f"Document parsing failed with status: {result.status}"

            return f"Document processing timeout after {self.timeout_seconds} seconds. Job ID: {job_id}"

        except Exception as e:
            logger.error(f"Error processing document: {e}")
            return f"Error processing document: {str(e)}"

    def extract_structured_data_from_document(
            self,
            extraction_schema: Union[str, dict],
            document_path: Optional[str] = None,
            extraction_prompt: Optional[str] = None,
            extraction_model: str = "tensorlake",
            page_range: Optional[str] = None,
            skip_ocr: Optional[bool] = None,
            detect_signature: Optional[bool] = None,
    ) -> str:
        """Extract structured data from a document using a defined schema.

        This function extracts specific data fields from documents and returns them
        in a structured format based on the provided schema.

        Args:
            extraction_schema: JSON schema defining the data structure to extract
                              Can be a JSON string or dictionary
                              Example: '{"name": "string", "date": "date", "amount": "number"}'
            document_path: Path to the document file to parse. If not provided, uses document_path from Toolkit initializaion
            extraction_prompt: Custom prompt to guide the extraction process
                             Example: "Extract all personal information and contact details"
            extraction_model: Model to use for extraction - "tensorlake", "sonnet", or "gpt4omini"
            page_range: Specific pages to process (e.g., '1-5', '1,3,5', '10-end')
            skip_ocr: Skip OCR for text-based PDFs (overrides default if specified)
            detect_signature: Whether to detect signatures (overrides default if specified)

        Returns:
            str: Extracted structured data in JSON format, or error message if failed
        """
        if not self.api_key:
            return "Error: TENSORLAKE_API_KEY is not configured"

        # Use provided path or fall back to default
        doc_path = document_path or self.document_path
        if not doc_path:
            return "Error: No document path provided and no default path set"

        # Use provided values or fall back to defaults
        skip_ocr_val = skip_ocr if skip_ocr is not None else self.default_skip_ocr
        detect_sig = detect_signature if detect_signature is not None else self.default_detect_signature

        try:
            # Initialize DocumentAI client
            doc_ai = DocumentAI(api_key=self.api_key)

            # Upload document
            logger.info(f"Uploading document for structured extraction: {doc_path}")
            file_id = doc_ai.upload(path=doc_path)

            # Configure parsing options for structured extraction
            parsing_options = ParsingOptions()

            # Set important flags
            parsing_options.detect_signature = detect_sig

            # Set extraction options
            extraction_options = ExtractionOptions()

            # Parse schema if it's a string
            if isinstance(extraction_schema, str):
                try:
                    import json
                    schema_dict = json.loads(extraction_schema)
                    extraction_options.extraction_schema = schema_dict
                except json.JSONDecodeError:
                    return f"Error: Invalid JSON schema format: {extraction_schema}"
            else:
                extraction_options.extraction_schema = extraction_schema

            if extraction_prompt:
                extraction_options.extraction_prompt = extraction_prompt

            # Set extraction model
            if extraction_model.lower() == "sonnet":
                extraction_options.extraction_model_provider = ModelProvider.SONNET
            elif extraction_model.lower() == "gpt4omini":
                extraction_options.extraction_model_provider = ModelProvider.GPT4OMINI
            else:
                extraction_options.extraction_model_provider = ModelProvider.TENSORLAKE

            if skip_ocr_val:
                extraction_options.skip_ocr = skip_ocr_val

            parsing_options.extraction_options = extraction_options

            # Set page range if specified
            if page_range:
                parsing_options.page_range = page_range

            # Start parsing job
            logger.info("Starting structured data extraction job")
            job_id = doc_ai.parse(file_id, options=parsing_options)

            # Poll for completion
            start_time = time.time()
            while time.time() - start_time < self.timeout_seconds:
                result = doc_ai.get_job(job_id)

                if result.status in ["pending", "processing"]:
                    logger.info(f"Extraction job {job_id} is {result.status}, waiting...")
                    time.sleep(5)
                elif result.status == "successful":
                    logger.info("Structured data extraction completed successfully")
                    # Return the extracted data
                    if hasattr(result, 'extracted_data') and result.extracted_data:
                        import json
                        return json.dumps(result.extracted_data, indent=2)
                    elif hasattr(result, 'content') and result.content:
                        return result.content
                    else:
                        return str(result)
                else:
                    return f"Structured data extraction failed with status: {result.status}"

            return f"Extraction processing timeout after {self.timeout_seconds} seconds. Job ID: {job_id}"

        except Exception as e:
            logger.error(f"Error extracting structured data: {e}")
            return f"Error extracting structured data: {str(e)}"
