"""
This script demonstrates how to use the DocumentAI class to load and parse a dataset of documents.
"""

import asyncio
import csv
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import (
    ChunkingStrategy,
    ParsingOptions,
    StructuredExtractionOptions,
)

load_dotenv()

document_ai = DocumentAI()

files = [
    "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/CHI_13.pdf",
    "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/CSCW_14_1.pdf",
    "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/CSCW_14_2.pdf",
    "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/CSCW_14_3.pdf",
]


class Author(BaseModel):
    """Author information for a research paper"""

    name: str = Field(description="Full name of the author")
    affiliation: str = Field(description="Institution or organization affiliation")


class Conference(BaseModel):
    """Conference or journal information"""

    name: str = Field(description="Name of the conference or journal")
    year: str = Field(description="Year of publication")
    location: str = Field(
        description="Location of the conference or journal publication"
    )


class Reference(BaseModel):
    """Reference to another publication"""

    author_names: List[str] = Field(
        description="List of author names for this reference"
    )
    title: str = Field(description="Title of the referenced publication")
    publication: str = Field(
        description="Name of the publication venue (journal, conference, etc.)"
    )
    year: str = Field(description="Year of publication")


class ResearchPaper(BaseModel):
    """Complete schema for extracting research paper information"""

    authors: List[Author] = Field(
        description="List of authors with their affiliations. Authors will be listed below the title and above the main text of the paper. Authors will often be in multiple columns and there may be multiple authors associated to a single affiliation."
    )
    conference_journal: Conference = Field(
        description="Conference or journal information"
    )
    title: str = Field(description="Title of the research paper")
    abstract: str = Field(description="Abstract or summary of the paper")
    keywords: List[str] = Field(
        description="List of keywords associated with the paper"
    )
    acm_classification: str = Field(description="ACM classification code or category")
    general_terms: List[str] = Field(description="List of general terms or categories")
    acknowledgments: str = Field(description="Acknowledgments section")
    references: List[Reference] = Field(
        description="List of references cited in the paper"
    )


async def main():
    """
    Main function
    """
    # Create a dataset, by specifying the document ingestion actions
    # The name of the dataset must be unique within a project so you
    # can retrieve the dataset later using the name.
    dataset = await document_ai.create_dataset_async(
        name="new_api",
        description="A dataset of documents",
        parsing_options=ParsingOptions(chunking_strategy=ChunkingStrategy.PAGE),
        structured_extraction_options=[
            StructuredExtractionOptions(
                schema_name="ResearchPaper",
                json_schema=ResearchPaper.model_json_schema(),
            )
        ],
        enrichment_options=None,
        page_classifications=None,
    )

    print(f"Dataset created: {dataset.dataset_id}")

    # Extend a existing dataset with some files. Tensorlake will automatically
    # parse the files or any other ingestion actions specified in the dataset.
    tasks = [
        document_ai.parse_dataset_file_async(dataset, file, wait_for_completion=False)
        for file in files
    ]
    parse_ids = await asyncio.gather(*tasks, return_exceptions=True)

    # Debug: Print parse results
    valid_parse_ids = []
    for idx, parse_id in enumerate(parse_ids):
        if isinstance(parse_id, Exception):
            print(f"Error in job {idx}: {parse_id}")  # Print exception details
        else:
            print(f"Job {idx} created successfully: {parse_id}")
            valid_parse_ids.append(parse_id)  # Only append valid jobs

    if not valid_parse_ids:
        print("No valid parse jobs created. Exiting.")
        return

    # Wait for completion of valid jobs
    print("Waiting for parsing jobs to complete...")
    wait_tasks = [
        document_ai.wait_for_completion_async(parse_id) for parse_id in valid_parse_ids
    ]
    results = await asyncio.gather(*wait_tasks, return_exceptions=True)

    # Process results
    successful_results = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Error waiting for job {valid_parse_ids[i]}: {result}")
        else:
            print(f"Job {valid_parse_ids[i]} completed successfully")
            successful_results.append((valid_parse_ids[i], result))

    # Save results to CSV
    if successful_results:
        csv_filename = f"{dataset.name}_results.csv"
        with open(csv_filename, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["parse_id", "file_name", "status", "structured_data", "chunks_count"]
            )

            for parse_id, result in successful_results:
                # Find corresponding file
                file_idx = (
                    valid_parse_ids.index(parse_id)
                    if parse_id in valid_parse_ids
                    else 0
                )
                file_name = (
                    files[file_idx].split("/")[-1]
                    if file_idx < len(files)
                    else f"unknown_{parse_id}"
                )

                # Extract key information
                status = result.status if hasattr(result, "status") else "unknown"
                chunks_count = (
                    len(result.chunks)
                    if hasattr(result, "chunks") and result.chunks
                    else 0
                )

                # Get structured data
                structured_data = ""
                if hasattr(result, "structured_data") and result.structured_data:
                    structured_data = (
                        str(result.structured_data[0].data)
                        if result.structured_data
                        else ""
                    )

                writer.writerow(
                    [parse_id, file_name, status, structured_data, chunks_count]
                )

        print(f"Results saved to {csv_filename}")
    else:
        print("No successful results to save.")


if __name__ == "__main__":
    asyncio.run(main())
