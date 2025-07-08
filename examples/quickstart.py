import json

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.parse import (
    ChunkingStrategy,
    ExtractionOptions,
    ParsingOptions,
    TableOutputMode,
    TableParsingStrategy,
)

# Initialize Tensorlake with your API key
api_key = "your_tensorlake_api_key"
doc_ai = DocumentAI(api_key)

# Use this already uploaded file for testing
file_id = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf"

# If you want to upload your own file, uncomment the following lines:
# file_path = "path_to_your_file.pdf"
# file_id = doc_ai.upload(file_path)

# Configure parsing options
options = ParsingOptions(
    page_number=1,
    chunk_strategy=ChunkingStrategy.NONE,
    table_parsing_strategy=TableParsingStrategy.TSR,
    table_output_mode=TableOutputMode.MARKDOWN,
    detect_signature=True,
    extraction_options=ExtractionOptions(
        schema="""{"properties":{"buyer":{"properties":{"buyer_name":{"type":"string"},"buyer_signature_date":{"description":"Date and time (if both are available) that the buyer signed.","type":"string"},"buyer_signed":{"description":"Determine if the buyer signed the agreement","type":"boolean"}},"type":"object"},"seller":{"properties":{"seller_name":{"type":"string"},"seller_signature_date":{"description":"Date and time (if both are available) that the seller signed.","type":"string"},"seller_signed":{"description":"Determine if the seller signed the agreement","type":"boolean"}},"type":"object"}},"title":"leasing_agreement","type":"object"}""",
        skip_ocr=True,
    ),
)

# Parse the document
job_id = doc_ai.parse(file_id, options)

# Wait for the job to complete
result = doc_ai.get_job(job_id)

while True:
    result = doc_ai.get_job(job_id)
    if result.status == "successful":
        break

# Save the result
with open("output.json", "w") as f:
    json.dump(result.model_dump(), f, indent=2)

print("✅ Output saved to output.json")
