import json

from pydantic import BaseModel

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import (
    ParsingOptions,
    StructuredExtractionOptions,
    TableOutputMode,
    TableParsingFormat,
)

TENSORLAKE_API_KEY = "tl_apiKey_XXXXX"

doc_ai = DocumentAI(api_key=TENSORLAKE_API_KEY)


class PaperSchema(BaseModel):
    """
    Paper schema for the Document AI API.
    """

    references: list[str]
    authors: list[str]
    title: str
    abstract: str


# Skip this if you are passing a pre-signed URL to the parse method or pass an external URL
file_id = doc_ai.upload(path="/path/to/files")

# Configure parsing options
parsing_options = ParsingOptions(
    table_parsing_format=TableParsingFormat.VLM,
    table_output_mode=TableOutputMode.MARKDOWN,
)

structured_extraction_options = StructuredExtractionOptions(
    schema_name="Research Paper", json_schema=PaperSchema
)

# Parse and extract structured data
result = doc_ai.parse_and_wait(
    file_id,  # You can pass in a publicly accessible URL instead of a file_id
    # "https://arxiv.org/pdf/2409.13148",
    parsing_options=parsing_options,
    structured_extraction_options=[structured_extraction_options],
)

# Print the structured data output
print(json.dumps(result.structured_data[0].data, indent=2))

# Get the markdown from extracted data
for index, chunk in enumerate(result.chunks):
    print(f"Chunk {index}:")
    print(chunk.content)
