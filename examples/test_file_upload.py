from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai.file import upload_file_sync
from tensorlake.documentai.parser import DocumentParser, ParsingOptions
from itertools import batched
import time

api_key = "tl_apiKey_bhGmRwFpbGnfmpN7Jt9fC_ENC4x_A-yf9fLEe70fs4488D70ZtOH"

document_parser = DocumentParser(api_key=api_key)

loader = LocalDirectoryLoader("/Users/diptanuc/Downloads/unextractable", file_extensions=[".pdf"])

files = loader.load()
file_ids = {}

job_ids = {}

for files in batched(files, 10):
    for file in files:
        time.sleep(1)
        file_id = upload_file_sync(file.path, api_key=api_key) 
        print(f"uploading file {file.path} size {file.file_size/2048} mb")
        print(file_id)
        file_ids[file.name] = file_id

for file_name, file_id in file_ids.items():
    job_id = document_parser.parse_document(file_id, ParsingOptions())
    print(job_id)
    job_ids[job_id] = (file_name, file_id)

import csv
csv_file = "job_ids.csv"
with open(csv_file, mode='w') as file:
    writer = csv.writer(file)
    writer.writerow(["job_id", "file_name", "file_id"])
    for job_id, (file_name, file_id) in job_ids:
        writer.writerow([job_id, file_name, file_id])
print(file_ids)
print(job_ids)




