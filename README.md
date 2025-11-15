<h1 align="center">
    <img width="1100" height="259" alt="Group 39884" src="https://github.com/user-attachments/assets/ac9adfc2-53cb-427e-ad6a-91394cdee961" />

</h1>

<p align="center">Get high quality data from Documents fast, and deploy scalable serverless Data Processor APIs</p>
<div align="center">


[![PyPI Version](https://img.shields.io/pypi/v/tensorlake)](https://pypi.org/project/tensorlake/)
[![Python Support](https://img.shields.io/pypi/pyversions/tensorlake)](https://pypi.org/project/tensorlake/)
[![License](https://img.shields.io/github/license/tensorlakeai/tensorlake)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-tensorlake.ai-blue)](https://docs.tensorlake.ai)
[![Slack](https://img.shields.io/badge/slack-TensorlakeCloud-purple?logo=slack)](https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg)

TensorLake transforms unstructured documents into AI-ready data through Document Ingestion APIs and enables building scalable data processing pipelines with a serverless workflow runtime. 

![gh_animation](https://github.com/user-attachments/assets/bc57d5f5-c745-4a36-926a-d85767b9115e)
</div>

## Features

- **Document Ingestion** - Parse documents (PDFs, DOCX, spreadsheets, presentations, images, and raw text) to markdown or extract structured data with schemas. This is powered by Tensorlake's state of the art Layout Detection and Table Recognition models.

- **Serverless Workflows** - Build and deploy data ingestion and orchestration APIs using Durable Functions in Python that scales automatically on fully managed infrastructure. The requests to workflows automatically resume from failure checkpoints and scale to zero when idle.
---

## Document Ingestion Quickstart

### Installation

Install the SDK and get an API Key.

```bash
pip install tensorlake
```

Sign up at [cloud.tensorlake.ai](https://cloud.tensorlake.ai/) and get your API key.

### Parse Documents

```python
from tensorlake.documentai import DocumentAI, ParseStatus

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

### Customize Parsing

Various aspect of Document Parsing, such as detecting strike through lines, table output mode, figure and table summarization can be customized. The API is [documented here](https://docs.tensorlake.ai/document-ingestion/parsing/read#options-for-parsing-documents).

```python
from tensorlake.documentai import DocumentAI, ParsingOptions, EnrichmentOptions, ParseStatus, ChunkingStrategy, TableOutputMode

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

### Structured Extraction

Extract specific data fields from documents using JSON schemas or Pydantic models:

#### Using Pydantic Models
```python
from tensorlake.documentai import DocumentAI, StructuredExtractionOptions, ParseStatus
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

### Learn More
* [Document Parsing Guide](https://docs.tensorlake.ai/document-ingestion/parsing/read)
* [Structured Output Guide](https://docs.tensorlake.ai/document-ingestion/parsing/structured-extraction)
* [Page Classification](https://docs.tensorlake.ai/document-ingestion/parsing/page-classification)
* [Signature Detection](https://docs.tensorlake.ai/document-ingestion/parsing/signature)

## Data Workflows

Workflows enables building and deploying workflow APIs. The workflow APIs are exposed as HTTP Endpoints.Functions in workflows can do anything from calling a web service to loading a data model into a GPU to run inference.

### Workflows Quickstart

Define a workflow by implementing its data transformation steps as Python functions decorated with `@tensorlake_function()`.
Connect the outputs of a function to the inputs of another function using edges in a `Graph` object, which represents the full workflow.

The example below creates a workflow with the following steps:

1. Generate a sequence of numbers from 0 to the supplied value.
2. Compute square of each number.
3. Sum all the squares.
4. Send the sum to a web service.

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
g = Graph(
    name="example_workflow",
    start_node=generate_sequence,
    description="Example workflow",
)
g.add_edge(generate_sequence, squared)
g.add_edge(squared, sum_all)
g.add_edge(sum_all, send_to_web_service)

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
run_workflow(g)
```

Run the workflow locally:

```bash
python examples/readme_example.py
```

In console output you can see that the workflow computed the sum and got a response from the web service.
Running a workflow locally is convenient during its development. There's no need to wait until the workflow sgets deployed to see how it works.

#### Running on Tensorlake Cloud

To run the workflow on tensorlake cloud it first needs to get deployed there.

1. Set `TENSORLAKE_API_KEY` environment variable in your shell session:
```bash
export TENSORLAKE_API_KEY="Paste your API key here"
```
2. Deploy the workflow to Tensorlake Cloud:
```bash
tensorlake deploy examples/readme_example.py
```
3. The following code was added to the workflow file to run it on Tensorlake Cloud:
```python
from tensorlake import RemoteGraph

cloud_workflow = RemoteGraph.by_name("example_workflow")
run_workflow(cloud_workflow)
```
4. Run the workflow on Tensorlake Cloud:

```bash
python examples/readme_example.py
```

## Learn more about workflows 

* [Serverless Workflows Documentation](https://docs.tensorlake.ai/applications/quickstart)
* [Key programming concepts in Tensorlake Workflows](https://docs.tensorlake.ai/applications/compute)
* [Dependencies and container images in Tensorlake Workflows](https://docs.tensorlake.ai/applications/images)
* [Open Source Workflow Compute Engine](https://docs.tensorlake.ai/opensource/indexify)
