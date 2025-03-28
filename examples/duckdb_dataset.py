from typing import List

from pydantic import BaseModel, Field
from tensorlake.connectors.duckdb import DuckDbConnector
from tensorlake.data_loaders import FileMetadata
from tensorlake.documentai import DocumentAI, ParsingOptions, DatasetOptions, Job
from tensorlake.documentai.datasets import Dataset
from tensorlake.documentai.parse import TableOutputMode, TableParsingStrategy, ExtractionOptions


class LoanSchema(BaseModel):
    account_number: str = Field(description="Account number of the customer")
    customer_name: str = Field(description="Name of the customer")
    amount_due: str = Field(description="Total amount due in the current statement")
    due_date: str = Field(description="Due Date")


def create_dataset(dataset_name: str, doc_ai: DocumentAI) -> Dataset:
    # Duckdb needs extraction options, we write structured output as csv.
    extraction_options = ExtractionOptions(schema=LoanSchema)
    parsing_options = ParsingOptions(extraction_options=extraction_options)

    options = DatasetOptions(
        name=dataset_name,
        options=parsing_options,
    )

    print(options.options.extraction_options)

    dataset: Dataset = doc_ai.create_dataset(dataset=options, ignore_if_exists=True)

    print(dataset.id, dataset.name)

    return dataset


def ingest_file(file_path: str, dataset: Dataset) -> Job:
    from tensorlake.documentai import IngestArgs

    job = dataset.ingest(IngestArgs(file_path=file_path))

    return job


def load_data_into_dataset(loaded_files: List[FileMetadata], doc_ai: DocumentAI) -> List[str]:
    file_ids = []
    for file in loaded_files:
        print(file)
        file_id = doc_ai.upload(file.path)
        file_ids.append(file_id)

    return file_ids

def parse_data(file_ids: List[str], doc_ai: DocumentAI) -> List[str]:
    options = ParsingOptions(
        table_output_mode=TableOutputMode.MARKDOWN,
        table_parsing_strategy=TableParsingStrategy.VLM,
        page_range='1-2'
    )

    job_ids = []
    for file_id in file_ids:
        job_id = doc_ai.parse(file_id, options)
        job_ids.append(job_id)

    return job_ids


def export():
    pass


if __name__ == '__main__':
    duckdb_connector = DuckDbConnector(db_name='st-test-db-1')

    doc_ai: DocumentAI = DocumentAI(api_key="tl_apiKey_rWKGR8KNJjjLfwc8hL8tG_G_PdXylnUAAH-cnx14G9LrTdjlOHhj")

    dataset: Dataset = create_dataset(dataset_name="st-duckdb-test-1", doc_ai=doc_ai)

    # TODO fix the api, we are returning job_id and not Job
    # job_id = ingest_file(file_path="/Users/stangirala/git/docs-test/dataset/papers/Gemma3Report.pdf", dataset=dataset)
    # print(job_id)

    ingest_count = dataset.export_dataset(connector=duckdb_connector)