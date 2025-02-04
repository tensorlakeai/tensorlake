from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai.file import Files
from tensorlake.documentai.parser import DocumentParser, ParsingOptions

api_key = "tl_XXXX"

document_parser = DocumentParser(api_key=api_key)

loader = LocalDirectoryLoader("/path/docs/folder", file_extensions=[".pdf"])

files = Files(api_key=api_key)

all_files = loader.load()
file_ids = {}

job_ids = {}

for file in all_files:
    file_id = files.upload(file)
    file_ids[file.name] = file_id

for file_name, file_id in file_ids.items():
   job_id = document_parser.parse(file_id, ParsingOptions())
   print(job_id)
   job_ids[job_id] = (file_name, file_id)

import csv
csv_file = "job_ids.csv"
with open(csv_file, mode='w') as file:
   writer = csv.writer(file)
   writer.writerow(["job_id", "file_name", "file_id"])
   for job_id, (file_name, file_id) in job_ids.items():
       writer.writerow([job_id, file_name, file_id])




