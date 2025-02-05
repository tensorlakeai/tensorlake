"""
Example of how to parse all documents in a folder asynchronously using the Document AI API.
"""

import asyncio
import csv

from requests.exceptions import HTTPError

from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai import DocumentAI, ParsingOptions

TENSORLAKE_API_KEY = "tl_apiKey_XXXXXXX"

document_parser = DocumentAI(api_key=TENSORLAKE_API_KEY)

FILES_DIR = "/path/to/files"
loader = LocalDirectoryLoader(FILES_DIR, file_extensions=[".pdf"])

all_files = loader.load()
file_ids = {}

job_ids = {}


async def upload_files():
    """
    Upload all files in the folder asynchronously
    """
    tasks = []
    for file in all_files:
        tasks.append(upload_single_file(file))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    for file, result in zip(all_files, results):
        if isinstance(result, str):
            file_ids[file.name] = result
        else:
            print(f"Error uploading file {file.path}: {result}")


async def upload_single_file(file):
    """
    Upload a single file asynchronously
    """
    try:
        return await document_parser.upload_async(file.path)
    except HTTPError as e:
        return e


async def parse_files():
    """
    Extract text from all files in the folder asynchronously
    """

    for file_name, file_id in file_ids.items():
        job_id = await document_parser.parse_async(file_id, ParsingOptions())
        print(job_id)
        job_ids[job_id] = (file_name, file_id)


async def main():
    """
    Main function
    """
    await upload_files()
    await parse_files()

    # Write job IDs to CSV
    csv_filename = "job_ids.csv"
    with open(csv_filename, mode="w", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["job_id", "file_name", "file_id"])
        for job_id, (file_name, file_id) in job_ids.items():
            writer.writerow([job_id, file_name, file_id])


asyncio.run(main())
