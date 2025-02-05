import time

from tensorlake.documentai import DocumentAI, ParsingOptions

API_KEY = "tl_XXXXX"

doc_ai = DocumentAI(api_key=API_KEY)
# Skip this if you are passing a pre-signed URL to the `DocumentParser`.
# or pass an external URL
file_id = doc_ai.upload(path="./examples/appliance-repair-invoice-2.pdf")

job_id = doc_ai.parse(file_id, options=ParsingOptions())

result = doc_ai.get_job(job_id=job_id)
print(f"job status: {result.status}")
while True:
    if result.status == "processing":
        print("waiting 5s...")
        time.sleep(5)
        result = doc_ai.get_job(job_id)
        print(f"job status: {result.status}")
    else:
        if result.status == "successful":
            print(result)
        break
