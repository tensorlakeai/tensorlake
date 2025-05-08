<a name="readme-top"></a>
# Tensorlake SDK

[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD) ![PyPI - Version](https://img.shields.io/pypi/v/tensorlake)

Tensorlake provides Document Ingestion APIs and a runtime to build and deploy data workflows on a fully managed compute infrastructure including GPUs.

## Quick Start

1. Install the SDK
```bash
pip install tensorlake
```

2. Sign up and get an Tensorlake [API Key](https://cloud.tensorlake.ai/)

## Document Ingestion

Document Ingestion APIs enable building RAG or Knowledge Assistants from information in PDFs, Docx or Presentations. It offers mainly the following capabilities - 

1. Document Parsing - Converts documents to text, and optionally chunk them. It can also extract information from Figure, Charts and Tables.

2. Structured Extraction - Extracts JSON from documents, guided by JSON schemas or Pydantic models.

## Quickstart 

If you want to dive into code, here is an [example](examples/readme_documentai.py).

#### Document Parsing

Convert a PDF to markdown and chunk it. The API has no limits of file size or number of pages in a document.

```python
from tensorlake.documentai import DocumentAI, ParsingOptions

doc_ai = DocumentAI(api_key="xxxx")

# Skip the upload step, if you are passing pre-signed URLs or HTTPS accessible files.
file_id = doc_ai.upload(path="/path/to/file.pdf")

# Get a Job ID back, and poll it to get the results.
job_id = doc_ai.parse(file_id, options=ParsingOptions())
```

The default chunking strategy is by Page, you can change the chunking strategy, the prompts for table and figure summarization by configuring `ParsingOptions`. The API is [documented here](https://docs.tensorlake.ai/documentai/parsing#parse-api-reference)

#### Getting Back Parsed Data

Document AI APIs are async to be able to handle large volumes of documents with many pages. You can use a Job ID to retrieve results, or configure a webhook endpoint to receive updates.

```python

from tensorlake.documentai import Job

data: Job = doc_ai.get_job(job_id="job-xxxx")
```

The SDK includes [Pydantic models](src/tensorlake/documentai/common.py) that describes Document chunks, and individual page elements(including bounding boxes).

#### Structured Extraction 

Extract structured data from a document.

```python
from tensorlake.documentai import ParsingOptions, ExtractionOptions
from pydantic import BaseModel, Field

# Provide a schema to guide structured extraction.
class LoanSchema(BaseModel):
    account_number: str = Field(description="Account number of the customer")
    customer_name: str = Field(description="Name of the customer")
    amount_due: str = Field(description="Total amount due in the current statement")
    due_data: str = Field(description="Due Date")

options = ParsingOptions(
    extraction_options=ExtractionOptions(schema=LoanSchema)
)

job_id = doc_ai.parse(file_id, options=options)
```

Structured Extraction is guided by the provided schema. We support Pydantic Models as well JSON Schema. All the levers for structured extraction are (documented here)[https://docs.tensorlake.ai/api-reference/extract/extract-file-async].

We recommend adding a description to each field in the schema, as it helps the model to learn the context of the field.

#### Datasets 

Datasets are a named collection that you can attach some ingestion actions, such as document parsing or structured extraction. These operations are automatically applied whenever new files are uploaded to the datasets.

1. Create a Dataset 
```python
from tensorlake.documentai import DatasetOptions, ParsingOptions, OutputFormat, TableOutputMode, TableParsingStrategy
    dataset = await doc_ai.create_dataset_async(
        DatasetOptions(
            name="My Dataset",
            description="A dataset of documents",
            options=ParsingOptions(
                format=OutputFormat.MARKDOWN,
                table_output_mode=TableOutputMode.JSON,
                table_parsing_strategy=TableParsingStrategy.VLM,
            ),
        )
    )
```

2. Add a document to a dataset 
```python
from tensorlake.documentai import IngestArgs

job = dataset.ingest(IngestArgs(file_path=file.path))
```

3. Retrieve Dataset output and metadata 
```python
items = dataset.items()
```

A dataset can be in any of these states - `idle`, `processing`. You can also configure a webhook to receive updates for each file that is processed.

## Webhooks

You can get notified by Tensorlake when documents are ingested. Here is a [code example](examples/webhook.py).

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

## Learn more

* [More examples](examples/)
* [Tensorlake Documentation](https://docs.tensorlake.ai)
* [Serverless Workflows Documentation](https://docs.tensorlake.ai/serverless/overview)
* [Key programming concepts in Tensorlake Workflows](https://docs.tensorlake.ai/serverless/key-concepts)
* [Dependencies and container images in Tensorlake Workflows](https://docs.tensorlake.ai/serverless/dependencies)
* [Open Source Indexify documentation for self-hosting](https://docs.tensorlake.ai/opensource/indexify)
