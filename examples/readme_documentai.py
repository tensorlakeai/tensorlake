import time
from tensorlake.documentai import DocumentParser, Files, ParsingOptions, Jobs


API_KEY = "tl_XXXXX"

files = Files(api_key=API_KEY)
file_id = files.upload(path="./examples/appliance-repair-invoice-2.pdf")

parser = DocumentParser(api_key=API_KEY)
job_id = parser.parse(file_id, options=ParsingOptions())

jobs_client = Jobs(api_key=API_KEY)

result = jobs_client.get(job_id=job_id)
print(f"job status: {result.status}")
while True:
    if result.status == "processing":
        print("waiting 5s...")
        time.sleep(5)
        result = jobs_client.get(job_id)
        print(f"job status: {result.status}")
    else:
        if result.status == "successful":
            print(result)
        break
