"""
This script demonstrates how to use the DocumentAI class to load and parse a dataset of documents.
"""

import asyncio
import csv

from pydantic import BaseModel, Field

from tensorlake.data_loaders import LocalDirectoryLoader
from tensorlake.documentai import (
    DatasetOptions,
    DocumentAI,
    ExtractionOptions,
    IngestArgs,
    ParsingOptions,
)

TENSORLAKE_API_KEY = "tl_apiKey_LBmFWTkrhpQFLzbBPwRQJ_QYbZs2iqdC5DncovbMGG_t7Wr9JsDs"

document_ai = DocumentAI(api_key=TENSORLAKE_API_KEY)

FILES_DIR = "/Users/miguelhernandez/Downloads/papers"
loader = LocalDirectoryLoader(FILES_DIR, file_extensions=[".pdf"])

files = loader.load()


class Transaction(BaseModel):
    date: str = Field(
        description="The date of the transaction. Dates should be formatted as dd/mm/yyyy"
    )
    description: str = Field(description="The description of the transaction")
    amount: float = Field(
        description="The amount of the transaction. Include the currency symbol"
    )
    transaction_type: str = Field(
        description="The type of the transaction. Debit or Credit"
    )


class Statement(BaseModel):
    beginning_balance: float = Field(
        description="The beginning balance of the statement. Include the currency symbol"
    )
    ending_balance: float = Field(
        description="The ending balance of the statement. Include the currency symbol"
    )
    account_number: str = Field(description="The account number")
    statement_start_date: str = Field(
        description="The start date of the statement. Dates should be formatted as dd/mm/yyyy"
    )
    statement_end_date: str = Field(
        description="The end date of the statement. Dates should be formatted as dd/mm/yyyy"
    )
    transactions: list[Transaction] = Field(
        description="The transactions in the statement"
    )


async def main():
    """
    Main function
    """
    # Create a dataset, by specifying the document ingestion actions
    # The name of the dataset must be unique within a project so you
    # can retrieve the dataset later using the name.
    dataset = await document_ai.create_dataset_async(
        DatasetOptions(
            name="new_api",
            description="A dataset of documents",
            options=ParsingOptions(
                extraction_options=ExtractionOptions(
                    model=Statement,
                )
            ),
        ),
        ignore_if_exists=True,
    )

    print(f"Dataset created: {dataset.id}")

    # Extend a existing dataset with some files. Tensorlake will automatically
    # parse the files or any other ingestion actions specified in the dataset.
    tasks = [dataset.ingest_async(IngestArgs(file_path=file.path)) for file in files]
    job_ids = await asyncio.gather(*tasks, return_exceptions=True)

    # Debug: Print job results
    valid_job_ids = []
    for idx, job_id in enumerate(job_ids):
        if isinstance(job_id, Exception):
            print(f"Error in job {idx}: {job_id}")  # Print exception details
        else:
            print(f"Job {idx} created successfully: {job_id}")
            valid_job_ids.append(job_id)  # Only append valid jobs

    # Proceed only with valid jobs
    # You can wait for the completion of the jobs using the `wait_for_completion_async` method
    wait_tasks = [
        document_ai.wait_for_completion_async(job_id) for job_id in valid_job_ids
    ]
    await asyncio.gather(*wait_tasks)

    # Retrieve the outputs of the dataset
    # The output includes the job id and the extracted contents
    items = {}
    items_page = await dataset.items_async()
    for key_info, data in items_page.items.items():
        items[key_info] = data.model_dump_json()

    cursor = items_page.cursor
    while cursor is not None:
        # TODO This isn't working
        items_page = await dataset.items_async(cursor=cursor)
        for key_info, data in items_page.items.items():
            items[key_info] = data.model_dump_json()
        cursor = items_page.cursor

    csv_filename = f"{dataset.name}.csv"
    with open(csv_filename, "w", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["job_id", "file_name", "output"])
        for key_info, data in items.items():
            writer.writerow([key_info.id, key_info.file_name, data])


asyncio.run(main())
