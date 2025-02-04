import time
from tensorlake.documentai import DocumentParser, Files, ParsingOptions, Jobs


API_KEY = "tl_apiKey_CTmmqmqgQhwLtgHGwffWz_iNPyz1PRW5n9n3lpI-XPxEEjZq_WZj"

# files = Files(api_key=API_KEY)
# file_id = files.upload(path="./examples/appliance-repair-invoice-2.pdf")

# parser = DocumentParser(api_key=API_KEY)
# job_id = parser.parse(file_id, options=ParsingOptions())
# print(job_id)

job_id="job-gP7LthGQNB8d9B6GgJLK9"

jobs_client = Jobs(api_key=API_KEY)

result = jobs_client.get(job_id=job_id)
print(result)
while True:
    if result.status == "processing":
        print("waiting 5s...")
        time.sleep(5)
        result = jobs_client.get(job_id)
    else:
        print(result.status)
        if result.status == "successful":
            print(result)
        break
