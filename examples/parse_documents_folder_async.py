"""
Example of how to parse all documents in a folder asynchronously using the Document AI API.
"""

import asyncio
import csv

from dotenv import load_dotenv
from requests.exceptions import HTTPError

from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai import DocumentAI

load_dotenv()

document_parser = DocumentAI()

FILES_DIR = "/path/to/files"
loader = LocalDirectoryLoader(FILES_DIR, file_extensions=[".pdf"])
all_files = loader.load()

file_ids = {}
parse_ids = {}


async def upload_files():
    """Upload all files in the folder asynchronously"""
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
    """Upload a single file asynchronously"""
    try:
        return await document_parser.upload_async(file.path)
    except HTTPError as e:
        return e


async def parse_files():
    """Parse text from all uploaded files asynchronously"""

    for file_name, file_id in file_ids.items():
        parse_id = await document_parser.parse_async(file_id)
        print(f"Started parsing {file_name}: {parse_id}")
        parse_ids[parse_id] = (file_name, file_id)


async def wait_for_completion():
    """Wait for all parse jobs to complete and get results"""
    results = {}

    for parse_id, (file_name, file_id) in parse_ids.items():
        result = await document_parser.wait_for_completion_async(parse_id)
        results[parse_id] = {
            "file_name": file_name,
            "file_id": file_id,
            "result": result,
        }
        print(f"Completed {file_name}: {result.status}")

    return results


async def main():
    """Main function"""

    print("Starting document processing...")

    # Upload all files
    await upload_files()
    print(f"Uploaded {len(file_ids)} files successfully")

    # Start parsing all files
    print("Starting parse jobs...")
    await parse_files()
    print(f"Started {len(parse_ids)} parse jobs")

    # Wait for all jobs to complete
    print("Waiting for completion...")
    results = await wait_for_completion()

    # Write results to CSV
    csv_filename = "parse_results.csv"
    with open(csv_filename, mode="w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["parse_id", "file_name", "file_id"])

        for parse_id, result in results.items():
            writer.writerow(
                [parse_id, result["file_name"], result["file_id"], result["result"]]
            )

    print(f"Results written to {csv_filename}")


asyncio.run(main())
