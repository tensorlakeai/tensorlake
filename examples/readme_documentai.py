import time

from pydantic import BaseModel

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.parse import ParsingOptions, ExtractionOptions

class PaperSchema(BaseModel):
    """
    Paper schema for the Document AI API.
    """
    references: list[str]
    authors: list[str]
    title: str
    abstract: str



API_KEY = "tl_XXXXX"

doc_ai = DocumentAI(api_key=API_KEY)
# Skip this if you are passing a pre-signed URL to the `DocumentParser`.
# or pass an external URL

file_id = doc_ai.upload(path="./examples/appliance-repair-invoice-2.pdf")

job_id = doc_ai.parse(file_id, options=ParsingOptions())

job_id = doc_ai.parse(
    file_id,
    options=ParsingOptions(extraction_options=ExtractionOptions(model=PaperSchema)),
)
print(f"job id: {job_id}")
result = doc_ai.get_job(job_id=job_id)
print(f"job status: {result.status}")
while True:
    if result.status in ["pending", "processing"]:
        print("waiting 5s...")
        time.sleep(5)
        result = doc_ai.get_job(job_id)
        print(f"job status: {result.status}")
    else:
        if result.status == "successful":
            # save the result to a file
            with open(f"{job_id}.json", "w", encoding="utf-8") as f:
                f.write(result.model_dump_json())
        break
