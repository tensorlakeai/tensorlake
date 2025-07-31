import json
import time

from dotenv import load_dotenv

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import (
    ParseStatus,
    ParsingOptions,
    StructuredExtractionOptions,
)
from tensorlake.documentai.models.enums import (
    ChunkingStrategy,
    TableOutputMode,
    TableParsingFormat,
)

load_dotenv()

doc_ai = DocumentAI()

# Use this already uploaded file for testing
file_url = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf"

# If you want to upload your own file, uncomment the following lines:
# file_path = "path_to_your_file.pdf"
# file_id = doc_ai.upload(file_path)

# Define the JSON schema for structured extraction
schema = {
    "title": "leasing_agreement",
    "type": "object",
    "properties": {
        "buyer": {
            "type": "object",
            "properties": {
                "buyer_name": {"type": "string"},
                "buyer_signature_date": {
                    "type": "string",
                    "description": "Date and time (if both are available) that the buyer signed.",
                },
                "buyer_signed": {
                    "type": "boolean",
                    "description": "Determine if the buyer signed the agreement",
                },
            },
        },
        "seller": {
            "type": "object",
            "properties": {
                "seller_name": {"type": "string"},
                "seller_signature_date": {
                    "type": "string",
                    "description": "Date and time (if both are available) that the seller signed.",
                },
                "seller_signed": {
                    "type": "boolean",
                    "description": "Determine if the seller signed the agreement",
                },
            },
        },
    },
}

# Configure parsing options
parsing_options = ParsingOptions(
    chunking_strategy=ChunkingStrategy.NONE,
    table_parsing_format=TableParsingFormat.TSR,
    table_output_mode=TableOutputMode.MARKDOWN,
    signature_detection=True,
)

structured_extraction_options = StructuredExtractionOptions(
    schema_name="Leasing Agreement", json_schema=schema, skip_ocr=True
)

# Parse the document
parse_id = doc_ai.parse(
    file_url,
    parsing_options=parsing_options,
    structured_extraction_options=[structured_extraction_options],
    page_range="1",
)

# Wait for the job to complete
result = doc_ai.get_parsed_result(parse_id)
while result.status in [ParseStatus.PENDING, ParseStatus.PROCESSING]:
    time.sleep(5)
    result = doc_ai.get_parsed_result(parse_id)
    if result.status == ParseStatus.SUCCESSFUL:
        print(f"Parse job {parse_id} is {result.status}")
        break
    print(f"Parse job {parse_id} is {result.status}, waiting...")

print(result)
