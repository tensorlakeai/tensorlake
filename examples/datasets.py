"""
This script demonstrates how to use the DocumentAI class to load and parse a dataset of documents.
"""

import asyncio
import csv

from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai import (
    DatasetExtendOptions,
    DatasetOptions,
    DocumentAI,
    OutputFormat,
    ParsingOptions,
    TableOutputMode,
    TableParsingStrategy,
)

TENSORLAKE_API_KEY = "tl_apiKey_***"

document_ai = DocumentAI(api_key=TENSORLAKE_API_KEY)

FILES_DIR = "/path/to/your/files"
loader = LocalDirectoryLoader(FILES_DIR, file_extensions=[".pdf"])

files = loader.load()


async def main():
    """
    Main function
    """
    dataset = await document_ai.create_dataset_async(
        DatasetOptions(
            name="My Dataset",
            description="A dataset of documents",
            parsing_options=ParsingOptions(
                format=OutputFormat.MARKDOWN,
                table_output_mode=TableOutputMode.JSON,
                table_parsing_strategy=TableParsingStrategy.VLM,
            ),
        )
    )

    print(f"Dataset created: {dataset.id}")

    tasks = [
        dataset.extend_async(DatasetExtendOptions(file_path=file.path))
        for file in files
    ]
    job_ids = await asyncio.gather(*tasks, return_exceptions=True)

    # Debug: Print job results
    valid_jobs = []
    for idx, job in enumerate(job_ids):
        if isinstance(job, Exception):
            print(f"Error in job {idx}: {job}")  # Print exception details
        else:
            print(f"Job {idx} created successfully: {job}")
            valid_jobs.append(job)  # Only append valid jobs

    # Proceed only with valid jobs
    wait_tasks = [document_ai.wait_for_completion_async(job) for job in valid_jobs]
    await asyncio.gather(*wait_tasks)

    outputs_page = await dataset.outputs_async()

    csv_filename = f"{dataset.name}.csv"
    with open(csv_filename, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["job_id", "output"])
        for job_id, data in outputs_page.outputs.items():
            writer.writerow([job_id, data.model_dump_json()])


asyncio.run(main())
