<a name="readme-top"></a>
# Tensorlake SDK

[![PyPI Version](https://img.shields.io/pypi/v/tensorlake)](https://pypi.org/project/tensorlake/)
[![Python Support](https://img.shields.io/pypi/pyversions/tensorlake)](https://pypi.org/project/tensorlake/)
[![License](https://img.shields.io/github/license/tensorlakeai/tensorlake)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-tensorlake.ai-blue)](https://docs.tensorlake.ai)
[![Slack](https://img.shields.io/badge/slack-TensorlakeCloud-purple?logo=slack)](https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg)

TensorLake transforms unstructured documents into AI-ready data through Document Ingestion APIs and enables building scalable data processing pipelines with a serverless workflow runtime. The platform handles the complexity of document parsing, data extraction, and workflow orchestration on fully managed infrastructure including GPU acceleration.

It consists of two core capabilities:

- **Document Ingestion** - Parse documents (PDFs, DOCX, spreadsheets, presentations, images, and raw text) to markdown, extract structured data with schemas, and manage document collections
- **Serverless Workflows** - Build and deploy data processing pipelines that scale automatically on cloud infrastructure
---

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Document Ingestion](#document-ingestion)
  - [Document Parsing](#document-parsing)
  - [Structured Extraction](#structured-extraction)
  - [Datasets](#datasets)
- [Custom Data Workflows](#custom-data-workflows)
  - [Creating Workflows](#quickstart-1)
  - [Local Development](#running-locally)
  - [Cloud Deployment](#running-on-tensorlake-cloud)
- [Webhooks](#webhooks)
- [Resources](#learn-more)

---

## Features

- Parse PDFs, DOCX, spreadsheets, presentations, images, and raw text into markdown
- Extract structured data using JSON Schema or Pydantic models
- Page classification, figure summarization, table extraction, signature detection
- Organize documents into auto-parsed datasets
- Deploy and run scalable workflows using a serverless cloud runtime

---

## Getting Started

### Installation

```bash
pip install tensorlake
```

### Get API Key

Sign up at [cloud.tensorlake.ai](https://cloud.tensorlake.ai/) for your API key.

## Document Ingestion

The Document Ingestion API converts unstructured documents into structured, processable formats. This is the foundation for building RAG systems, knowledge bases, and document analysis applications.

### Document Parsing

Convert documents to clean markdown or JSON while preserving layout, tables, and structure:

#### Quickstart

This uses default `ParsingOptions` to parse the document.

```python
from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import ParseStatus

doc_ai = DocumentAI(api_key="your-api-key")

# Upload and parse document
file_id = doc_ai.upload("/path/to/document.pdf")

# Get parse ID
parse_id = doc_ai.parse(file_id)

# Wait for completion and get results
result = doc_ai.wait_for_completion(parse_id)

if result.status == ParseStatus.SUCCESSFUL:
    for chunk in result.chunks:
        print(chunk.content)  # Clean markdown output
```

#### Document Parsing with custom Parsing Options

You can set custom parsing strategy for your document by configuring `ParsingOptions` and `EnrichmentOptions`. The API is documented [here](https://docs.tensorlake.ai/documentai/parsing#parse-api-reference)

```python
from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import ParsingOptions, EnrichmentOptions, ParseStatus
from tensorlake.documentai.models.enums import ChunkingStrategy, TableOutputMode

doc_ai = DocumentAI(api_key="your-api-key")

# Skip the upload step, if you are passing pre-signed URLs or HTTPS accessible files.
file_id = doc_ai.upload("/path/to/document.pdf")

# Configure parsing options
parsing_options = ParsingOptions(
    chunking_strategy=ChunkingStrategy.SECTION,
    table_output_mode=TableOutputMode.HTML,
    signature_detection=True
)

# Configure enrichment options
enrichment_options = EnrichmentOptions(
    figure_summarization=True,
    table_summarization=True
)

# Parse and wait for completion
result = doc_ai.parse_and_wait(
    file_id,
    parsing_options=parsing_options,
    enrichment_options=enrichment_options
)

if result.status == ParseStatus.SUCCESSFUL:
    for chunk in result.chunks:
        print(chunk.content)
```

**Supported Formats:** PDF, DOCX, PPTX, images, spreadsheets, handwritten notes

**Key Features:**
- Multiple chunking strategies (entire document, page, section, fragment)
- Table extraction and structure preservation
- Figure and table summarization
- Signature detection
- Strikethrough removal
- Reading order preservation
- No limits on file size or page count

**Getting Results:**
```python
from tensorlake.documentai.models import ParseStatus

result = doc_ai.get_parsed_result(parse_id)

if result.status == ParseStatus.SUCCESSFUL:
    # Access parsed content
    if result.chunks:
        for chunk in result.chunks:
            print(f"Page {chunk.page_number}: {chunk.content}")

    # Access structured data if configured
    if result.structured_data:
        for data in result.structured_data:
            print(f"Schema: {data.schema_name}")
            print(f"Data: {data.data}")

    # Access page layout information
    if result.pages:
        for page in result.pages:
            print(f"Page {page.page_number} has {len(page.page_fragments)} fragments")
```

> **Note:** Document AI APIs are async to be able to handle large volumes of documents with many pages. You can use a Parse ID to retrieve results, or configure a webhook endpoint to receive updates.

### Structured Extraction

Extract specific data fields from documents using JSON schemas or Pydantic models:

#### Using Pydantic Models
```python
from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import StructuredExtractionOptions, ParseStatus
from pydantic import BaseModel, Field

# Define Pydantic model
class InvoiceData(BaseModel):
    invoice_number: str = Field(description="Invoice number")
    total_amount: float = Field(description="Total amount due")
    due_date: str = Field(description="Payment due date")
    vendor_name: str = Field(description="Vendor company name")

doc_ai = DocumentAI(api_key="your-api-key")

# Passing https accessible file directly (no need to upload to Tensorlake)
file_id = "https://...."   # publicly available URL of the invoice data file

# Configure structured extraction using Pydantic model
structured_extraction_options = StructuredExtractionOptions(
    schema_name="Invoice Data",
    json_schema=InvoiceData  # Can pass Pydantic model directly
)

# Parse and wait for completion
result = doc_ai.parse_and_wait(
    file_id,
    structured_extraction_options=[structured_extraction_options]
)

if result.status == ParseStatus.SUCCESSFUL:
    print(result.structured_data)
```

#### Using JSON Schema
```python
# Define JSON schema directly
invoice_schema = {
    "title": "InvoiceData",
    "type": "object",
    "properties": {
        "invoice_number": {"type": "string", "description": "Invoice number"},
        "total_amount": {"type": "number", "description": "Total amount due"},
        "due_date": {"type": "string", "description": "Payment due date"},
        "vendor_name": {"type": "string", "description": "Vendor company name"}
    }
}

structured_extraction_options = StructuredExtractionOptions(
    schema_name="Invoice Data",
    json_schema=invoice_schema
)
```

Structured Extraction is guided by the provided schema. We support Pydantic Models as well JSON Schema. All the levers for structured extraction are documented [here](https://docs.tensorlake.ai/document-ingestion/parsing/structured-extraction).

We recommend adding a description to each field in the schema, as it helps the model to learn the context of the field.

### Datasets

Tensorlake Datasets are named collections of parse settings and results that allow you to apply ingestion actions, such as document parsing and structured extraction to any file parsed through the dataset. They are ideal for batch processing at scale.

When you create a dataset, you specify a configuration for parsing or extraction options. Every document added to the dataset inherits this configuration and is processed asynchronously by the Tensorlake backend.

> **Note:** You can attach webhooks to a dataset to receive status updates when documents are successfully processed.

#### 1. Create a Dataset
```python
from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import ParsingOptions, EnrichmentOptions
from tensorlake.documentai.models.enums import TableOutputMode, TableParsingFormat
from pydantic import BaseModel, Field

# Define schema for structured extraction
class DocumentSchema(BaseModel):
    title: str = Field(description="Document title")
    summary: str = Field(description="Document summary")

doc_ai = DocumentAI(api_key="your-api-key")

# Create dataset with configuration
dataset = doc_ai.create_dataset(
    name="My Dataset",
    description="A dataset of documents",
    parsing_options=ParsingOptions(
        table_output_mode=TableOutputMode.HTML,
        table_parsing_format=TableParsingFormat.VLM,
    ),
    enrichment_options=EnrichmentOptions(
        table_summarization=True,
        figure_summarization=True
    )
)
```

For async operation, use `create_dataset_async` instead of `create_dataset`.

#### 2. Add a document to a dataset
```python
# Parse a single file using dataset configuration
parse_id = doc_ai.parse_dataset_file(
    dataset,
    "/path/to/document.pdf",  # Or you can use URLs
    wait_for_completion=False  # Returns parse_id immediately
)

# Or wait for completion
result = doc_ai.parse_dataset_file(
    dataset,
    "/path/to/document.pdf",
    wait_for_completion=True  # Returns ParseResult
)
```

#### 3. Retrieve Dataset output and metadata
```python
# Get dataset information
dataset_info = doc_ai.get_dataset(dataset.dataset_id)
print(f"Dataset status: {dataset_info.status}")  # idle or processing

# List all parse results for this dataset
results = doc_ai.list_parse_results(dataset_name=dataset.name)
for result in results.items:
    print(f"Parse {result.parse_id}: {result.status}")
    if result.structured_data:
        print(f"Extracted data: {result.structured_data}")

# List all datasets in your project
datasets = doc_ai.list_datasets()
for ds in datasets.items:
    print(f"Dataset: {ds.name} - Status: {ds.status}")

# Update dataset configuration
updated_dataset = doc_ai.update_dataset(
    dataset,
    description="Updated description",
    parsing_options=ParsingOptions(
        table_output_mode=TableOutputMode.MARKDOWN  # Changed table output mode
    )
)
```

A dataset can be in any of these states - `idle`, `processing`. You can also configure a webhook to receive updates for each file that is processed.

## Custom Data Workflows

Workflows enables building and deploy data processing workflows in Python. Once deployed, the workflows are exposed as a REST API, and scale up on-demand to process data on the cloud. Functions in workflows can do anything from calling a web service to loading a data model into a GPU and running inference on it. Tensorlake will provision the required compute resources and run as many copies of a function as needed.

### Quickstart

Define a workflow by implementing its data transformation steps as Python functions decorated with `@tensorlake_function()`.
Connect the outputs of a function to the inputs of another function using edges in a `Graph` object, which represents the full workflow.

### Example

The example below creates a workflow with the following steps:

1. Generate a sequence of numbers from 0 to the supplied value.
2. Compute square of each number.
3. Sum all the squares.
4. Send the sum to a web service.

#### Code

```python
import os
import urllib.request
from typing import List, Optional

import click # Used for pretty printing to console.

from tensorlake import Graph, RemoteGraph, tensorlake_function

# Define a function for each workflow step


# 1. Generate a sequence of numbers from 0 to the supplied value.
@tensorlake_function()
def generate_sequence(last_sequence_number: int) -> List[int]:
    # This function impelements a map operation because it returns a list.
    return [i for i in range(last_sequence_number + 1)]


# 2. Compute square of each number.
@tensorlake_function()
def squared(number: int) -> int:
    # This function transforms each element of the sequence because it accepts
    # only a single int as a parameter.
    return number * number


# 3. Sum all the squares.
@tensorlake_function(accumulate=int)
def sum_all(current_sum: int, number: int) -> int:
    # This function implements a reduce operation.
    # It is called for each element of the sequence. The returned value is passed
    # to the next call in `current_sum` parameter. The first call gets `current_sum`=int()
    # which is 0. The return value of the last call is the result of the reduce operation.
    return current_sum + number


# 4. Send the sum to a web service.
@tensorlake_function()
def send_to_web_service(value: int) -> str:
    # This function accepts the sum from the previous step and sends it to a web service.
    url = f"https://example.com/?number={value}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as response:
        return response.read()


# Define the full workflow using Graph object
def create_workflow() -> Graph:
    g = Graph(
        name="example_workflow",
        start_node=generate_sequence,
        description="Example workflow",
    )
    g.add_edge(generate_sequence, squared)
    g.add_edge(squared, sum_all)
    g.add_edge(sum_all, send_to_web_service)
    return g


# Invoke the workflow for sequence [0..200].
def run_workflow(g: Graph) -> None:
    invocation_id: str = g.run(last_sequence_number=200, block_until_done=True)

    # Get the output of the the workflow (of its last step).
    last_step_output: str = g.output(invocation_id, "send_to_web_service")
    click.secho("Web service response:", fg="green", bold=True)
    click.echo(last_step_output[0])
    click.echo()

    # Get the sum.
    sum_output: str = g.output(invocation_id, "sum_all")
    click.secho("Sum:", fg="green", bold=True)
    click.echo(sum_output[0])
    click.echo()
```

#### Running locally

The workflow code is available at [examples/readme_example.py](examples/readme_example.py).
The following code was added there to create the workflow and run it locally on your computer:

```python
local_workflow: Graph = create_workflow()
run_workflow(local_workflow)
```

Run the workflow locally:

```bash
python examples/readme_example.py
```

In console output you can see that the workflow computed the sum and got a response from the web service.
Running a workflow locally is convenient during its development. There's no need to wait until the workflow
gets deployed to see how it works.

#### Running on Tensorlake Cloud

To run the workflow on tensorlake cloud it first needs to get deployed there.

1. Set `TENSORLAKE_API_KEY` environment variable in your shell session:
```bash
export TENSORLAKE_API_KEY="Paste your API key here"
```
2. Deploy the workflow to Tensorlake Cloud:
```bash
tensorlake-cli deploy examples/readme_example.py
```
3. The following code was added to the workflow file to run it on Tensorlake Cloud:
```python
def fetch_workflow_from_cloud() -> Optional[RemoteGraph]:
    return RemoteGraph.by_name("example_workflow")

cloud_workflow: RemoteGraph = fetch_workflow_from_cloud()
run_workflow(cloud_workflow)
```
4. Run the workflow on Tensorlake Cloud:

```bash
python examples/readme_example.py
```

#### Running on your own infrastructure

Tensorlake Workflows are based on an Open Source [Indexify](https://github.com/tensorlakeai/indexify)
and is fully compatible with it. You can setup your own Indexify cluster e.g. with Kubernetes
and run workflows on it.

Running workflows on Tensorlake Cloud comes with the following benefits:

* Automatically scale compute resources to the required number of workflow invocations.
* Pay only for compute resources used by the workflow. No need to pay for idle resources.
* Automated workflow deployments using a few CLI commands.
* High availability of Tensorlake Cloud.

## Webhooks

Get real-time notifications when document processing completes. Webhooks are configured at the project level in TensorLake Cloud and will notify your application about job status changes.

**Supported Events:**
- `tensorlake.document_ingestion.job.created` - Job started
- `tensorlake.document_ingestion.job.failed` - Job failed
- `tensorlake.document_ingestion.job.completed` - Job completed successfully

**Quick Setup:**
1. Go to your project's Webhooks tab in [TensorLake Cloud](https://cloud.tensorlake.ai)
2. Create a webhook with your endpoint URL
3. Select which events to receive
4. Use the provided secret for signature verification

**Webhook Payload Example:**
```json
{
    "job_id": "parse_XXX",
    "status": "successful",
    "created_at": "2023-10-01T12:00:00Z",
    "finished_at": "2023-10-01T12:05:00Z"
}
```

## Learn more

* [More examples](examples/)
* [Tensorlake Documentation](https://docs.tensorlake.ai)
* [Serverless Workflows Documentation](https://docs.tensorlake.ai/workflows/overview)
* [Key programming concepts in Tensorlake Workflows](https://docs.tensorlake.ai/workflows/functions)
* [Dependencies and container images in Tensorlake Workflows](https://docs.tensorlake.ai/workflows/dependencies)
* [Open Source Indexify documentation for self-hosting](https://docs.tensorlake.ai/opensource/indexify)
