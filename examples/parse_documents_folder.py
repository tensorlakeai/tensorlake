"""
Example of how to parse all the documents in a folder using the DocumentAI API.
"""

import csv

from requests.exceptions import HTTPError

from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai.file import Files
from tensorlake.documentai.parser import DocumentParser, ParsingOptions

TENSORLAKE_API_KEY = "tl_apiKey_************"

document_parser = DocumentParser(api_key=TENSORLAKE_API_KEY)

FILES_DIR = "/path/to/folder"
loader = LocalDirectoryLoader(FILES_DIR, file_extensions=[".pdf"])

files = Files(api_key=TENSORLAKE_API_KEY)

all_files = loader.load()
file_ids = {}
job_ids = {}

for file in all_files:
    try:
        file_id = files.upload(file.path)
        file_ids[file.name] = file_id
    except HTTPError as e:
        print(f"Error uploading file {file.path}: {e}")


for file_name, file_id in file_ids.items():
    job_id = document_parser.parse(file_id, ParsingOptions())
    print(job_id)
    job_ids[job_id] = (file_name, file_id)

CSV_FILENAME = "job_ids.csv"
with open(CSV_FILENAME, mode="w", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["job_id", "file_name", "file_id"])
    for job_id, (file_name, file_id) in job_ids.items():
        writer.writerow([job_id, file_name, file_id])
