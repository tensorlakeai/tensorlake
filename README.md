<a name="readme-top"></a>
# Tensorlake Cloud

[![Discord](https://dcbadge.vercel.app/api/server/VXkY7zVmTD?style=flat&compact=true)](https://discord.gg/VXkY7zVmTD)

Tensorlake provides Document Ingestion APIs and a runtime to build data workflows.

## Quick Start

```bash
pip install tensorlake
```

## Document Ingestion

Document Ingestion APIs enable building RAG or Knowledge Assistants from information in PDFs, Docx or Presentations. 

The API offers two main primitives - 
1. Document Parsing - Converts documents to text, and optionally chunk them. It can also extract information from Figure, Charts and Tables.

2. Structured Extraction - Extracts JSON from documents, guided by a provided schema.

## Quickstart 

#### Document Parsing

Convert a PDF to markdown and chunk it.

```python
from tensorlake.documentai import DocumentParser, Files, ParsingOptions

files = Files(api_key="xxxx")
file_id = files.upload(path="/path/to/file.pdf")

parser = DocumentParser(api_key="tl_xxx")
job_id = parser.parse_document(file_id, options=ParsingOptions())
```

This uses the default Parsing options, which chunks a document by page. We can summarize figures, charts and tables as well. You can change the chunking strategy, the prompts for summarization by changing the `ParsingOptions`. The API is [documented here](https://docs.tensorlake.ai/documentai/parsing).

#### Structured Extraction 

Extract structured data from a document.

```python
from tensorlake.documentai import StructuredExtractor, Files, ExtractionOptions
from pydantic import BaseModel, Field

class LoanDocumentSchema(BaseModel):
    account_number: str = Field(description="Account number of the customer")
    customer_name: str = Field(description="Name of the customer")
    amount_due: str = Field(description="Total amount due in the current statement")
    due_data: str = Field(description="Due Date")

files = Files(api_key="xxxx")
file_id = files.upload(path="/path/to/file.pdf")

parser = StructuredExtractor(api_key="tl_xxx")
job_id = parser.parse_document(file_id, options=ExtractionOptions(schema=LoanDocumentSchema))
```

This uses the default Parsing options, which chunks a document by page. We can summarize figures, charts and tables as well. You can change the chunking strategy, the prompts for summarization by changing the `ParsingOptions`. The API is [documented here](https://docs.tensorlake.ai/documentai/parsing).


## Serverless Workflows

Serverless Workflows allows to build and deploy custom data processing workflows as an API.

### Quickstart

Define a workflow by implementing its data transformation steps as Python functions decorated with `@tensorlake_function()`.
Connect the outputs of a function to the inputs of another function using edges in a `Graph` object, which represents the full workflow.

A function can do anything from calling a web service to loading a data model into a GPU and running inference on it. Tensorlake will
provision the required compute resources and run as many copies of a function as needed.

### Example workflow

The example below creates a workflow with the following steps:

1. Generate a sequence of numbers from 0 to the supplied value.
2. Compute square of each number.
3. Sum all the squares.
4. Send the sum to a web service.

#### Installation

Install Tensorlake SDK and CLI into your development environment:

```bash
pip install tensorlake
```

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

1. Register at [Tensorlake Cloud](https://cloud.tensorlake.ai).
2. Get an API key at [API keys page](https://cloud.tensorlake.ai/dashboard/api-keys).
3. Set `TENSORLAKE_API_KEY` environment variable in your shell session:
```bash
export TENSORLAKE_API_KEY="Paste your API key here"
```
4. Deploy the workflow to Tensorlake Cloud:
```bash
tensorlake-cli prepare examples/readme_example.py
tensorlake-cli deploy examples/readme_example.py
```
5. The following code was added to the workflow file to run it on Tensorlake Cloud:
```python
def fetch_workflow_from_cloud() -> Optional[RemoteGraph]:
    return RemoteGraph.by_name("example_workflow")

cloud_workflow: RemoteGraph = fetch_workflow_from_cloud()
run_workflow(cloud_workflow)
```
6. Run the workflow on Tensorlake Cloud:

```bash
python examples/readme_example.py
```

#### Running on your own infrastructure

Tensorlake Cloud is based on Open Source [Indexify](https://github.com/tensorlakeai/indexify)
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