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

Tensorlake is the platform for agentic applications. Build and deploy high throughput, durable, agentic applications and workflows in minutes, leveraging our best-in-class Document Ingestion API and compute platform for applications.

![Animation showing the Tensorlake Document Ingestion UI parsing an ACORD doc into Markdown](/assets/README-DocAI.gif)
</div>

## Features

- **[Document Ingestion](#document-ingestion-quickstart)** - Parse documents (PDFs, DOCX, spreadsheets, presentations, images, and raw text) to markdown or extract structured data with schemas. This is powered by Tensorlake's state of the art layout detection and table recognition models. Review our [benchmarks here](https://www.tensorlake.ai/blog/benchmarks).

- **[Agentic Applications](#build-durable-agentic-applications-in-python)** - Deploy Agentic Applications and AI Workflows using durable functions, with sandboxed and managed compute infrastructure that scales your agents with usage.

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
result = doc_ai.result(parse_id)

if result.status == ParseStatus.SUCCESSFUL:
    for chunk in result.chunks:
        print(chunk.content)  # Clean markdown output
```

### Customize Parsing

Configure chunking, table output, figure summarization, and more. [See all options](https://docs.tensorlake.ai/document-ingestion/parsing/read#options-for-parsing-documents).

```python
from tensorlake.documentai import DocumentAI, ParsingOptions, EnrichmentOptions, ChunkingStrategy, TableOutputMode

doc_ai = DocumentAI(api_key="your-api-key")
file_id = doc_ai.upload("/path/to/document.pdf")

result = doc_ai.parse_and_wait(
    file_id,
    parsing_options=ParsingOptions(
        chunking_strategy=ChunkingStrategy.SECTION,
        table_output_mode=TableOutputMode.HTML,
        signature_detection=True
    ),
    enrichment_options=EnrichmentOptions(
        figure_summarization=True,
        table_summarization=True
    )
)
```

### Structured Extraction

Extract specific data fields using Pydantic models or JSON schemas. [See docs](https://docs.tensorlake.ai/document-ingestion/parsing/structured-extraction).

```python
from tensorlake.documentai import DocumentAI, StructuredExtractionOptions
from pydantic import BaseModel, Field

class InvoiceData(BaseModel):
    invoice_number: str = Field(description="Invoice number")
    total_amount: float = Field(description="Total amount due")
    due_date: str = Field(description="Payment due date")
    vendor_name: str = Field(description="Vendor company name")

doc_ai = DocumentAI(api_key="your-api-key")

result = doc_ai.parse_and_wait(
    "https://example.com/invoice.pdf",  # Or use file_id from upload()
    structured_extraction_options=[StructuredExtractionOptions(
        schema_name="Invoice Data",
        json_schema=InvoiceData
    )]
)
print(result.structured_data)
```

### Learn More
* [Document Parsing Guide](https://docs.tensorlake.ai/document-ingestion/parsing/read)
* [Structured Output Guide](https://docs.tensorlake.ai/document-ingestion/parsing/structured-extraction)
* [Page Classification](https://docs.tensorlake.ai/document-ingestion/parsing/page-classification)
* [Signature Detection](https://docs.tensorlake.ai/document-ingestion/parsing/signature)

## Build Durable Agentic Applications in Python

Deploy agentic applications on a distributed runtime with automatic scaling and durable execution — applications restart from where they crashed automatically. You can build with any Python framework. Agents are exposed as HTTP APIs like web applications.

- **No Queues**: We manage state and orchestration
- **Zero Infra**: Write Python, deploy to Tensorlake
- **Progress Updates**: Applications can run for any amount of time and stream updates to users.

### Quickstart

Decorate your entrypoint with `@application()` and functions with `@function()` for checkpointing and sandboxed execution. Each function runs in its own isolated sandbox.

**Example**: City guide using OpenAI Agents with web search and code execution:

```python
from agents import Agent, Runner
from agents.tool import WebSearchTool, function_tool
from tensorlake.applications import application, function, Image

# Define the image with necessary dependencies
FUNCTION_CONTAINER_IMAGE = Image(base_image="python:3.11-slim", name="city_guide_image").run(
    "pip install openai openai-agents"
)

@function_tool
@function(
    description="Gets the weather for a city using an OpenAI Agent with web search",
    secrets=["OPENAI_API_KEY"],
    image=FUNCTION_CONTAINER_IMAGE,
)
def get_weather_tool(city: str) -> str:
    """Uses an OpenAI Agent with WebSearchTool to find current weather."""
    agent = Agent(
        name="Weather Reporter",
        instructions="Use web search to find current weather in Fahrenheit for the city.",
        tools=[WebSearchTool()],  # Agent can search the web
    )
    result = Runner.run_sync(agent, f"City: {city}")
    return result.final_output.strip()

@application(tags={"type": "example", "use_case": "city_guide"})
@function(
    description="Creates a guide with temperature conversion using function_tool",
    secrets=["OPENAI_API_KEY"],
    image=FUNCTION_CONTAINER_IMAGE,
)
def city_guide_app(city: str) -> str:
    """Uses an OpenAI Agent with function_tool to run Python code for conversion."""
    
    @function_tool
    def convert_to_celsius_tool(python_code: str) -> float:
        """Converts Fahrenheit to Celsius - runs as Python code via Agent."""
        return float(eval(python_code))
    
    agent = Agent(
        name="Guide Creator",
        instructions="Using the appropriate tools, get the weather for the purposes of the guide. If the city uses Celsius, call convert_to_celsius_tool to convert the temperature, passing in the code needed to convert the temperature to Celsius. Create a friendly guide that references the temperature of the city in Celsius if the city typically uses Celsius, otherwise reference the temperature in Fahrenheit. Only reference Celsius or Farenheit, not both.",
        tools=[get_weather_tool, convert_to_celsius_tool],  # Agent can execute this Python function
    )
    result = Runner.run_sync(agent, f"City: {city}")
    return result.final_output.strip()
```

> **Note**: This is a simplified version. See the complete example at [examples/readme_example/city_guide.py](examples/readme_example/city_guide.py) for the full implementation including activity suggestions and agent orchestration.

#### Deploy to Tensorlake Cloud

1. Set your API keys:
```bash
export TENSORLAKE_API_KEY="your-api-key"
tensorlake secrets set OPENAI_API_KEY "your-openai-key"
```

2. Deploy:
```bash
tensorlake deploy examples/readme_example/city_guide.py
```

#### Call via HTTP

```bash
# Invoke the application
curl https://api.tensorlake.ai/applications/city_guide_app \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  --json '"San Francisco"'
# Returns: {"request_id": "beae8736ece31ef9"}

# Get the result
curl https://api.tensorlake.ai/applications/city_guide_app/requests/{request_id}/output \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY"

# Stream results with SSE
curl https://api.tensorlake.ai/applications/city_guide_app \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  -H "Accept: text/event-stream" \
  --json '"San Francisco"'

# Send files
curl https://api.tensorlake.ai/applications/my_pdf_processor \
  -H "Authorization: Bearer $TENSORLAKE_API_KEY" \
  -H "Content-Type: application/pdf" \
  --data-binary @document.pdf
```

## Learn More

* [Applications Documentation](https://docs.tensorlake.ai/applications/quickstart)
* [Programming Concepts](https://docs.tensorlake.ai/applications/compute)
* [Dependencies & Images](https://docs.tensorlake.ai/applications/images)
* [Open Source Compute Engine](https://docs.tensorlake.ai/opensource/indexify)
