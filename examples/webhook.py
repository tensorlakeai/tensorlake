# 1. Configure a webhook by following the instructions in the [docs](https://docs.tensorlake.ai/webhooks)


# 2. Start a Parsing Job.
import time

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.parse import ParsingOptions, TableParsingStrategy

API_KEY = "tl_api_key_XXXX"

doc_ai = DocumentAI(api_key=API_KEY)

# Skip this if you are passing a pre-signed URL to the `DocumentParser`.
# or pass an external URL
# file_id = doc_ai.upload(path="/path/to/files")


job_id = doc_ai.parse(
    # file_id, # You can pass in a publicly accessible URL instead of a file_id
    "https://pub-157277cc11d64fb1a11f71cc52c688eb.r2.dev/invoice-example.pdf",
    options=ParsingOptions(
        table_parsing_strategy=TableParsingStrategy.VLM,
    ),
    deliver_webhook=True,
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
