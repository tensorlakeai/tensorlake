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

## Build Durable Agentic Applications in Python

Tensorlake's Agentic Runtime allows you to deploy agentic applications built in any framework on a districutred runtime, which scales them as they get requests. The platform has built in durable execution to let applications restart from where they crash automatically. 

**No Queues**: We manage internal state of applications and orchestration - no need for queues, background jobs and brittle retry logic.

**Zero Infra**: Write Python, deploy to Tensorlake.

### Agentic Applications Quickstart

Write an Application in Python, decorate the entrypoint of your application with `@application()` and the functions with `@function()` if you want their state to be checkpointed or run them in sandboxes. **Each Tensorlake function runs in its own isolated sandbox**, allowing you to safely execute code and use different dependencies per function.

The example below creates a city guide application using **OpenAI Agents with tool calls**. It demonstrates:

1. **Tool Calls**: Using OpenAI Agents with `WebSearchTool` to search the web and `function_tool` to execute Python code, including Tensorlake Functions.
2. **Sandboxed Execution**: Each `@function` runs in its own isolated environment with specified dependencies.
3. **Code Execution**: Agents can run Python code via `function_tool` within the sandbox.

```python
import os
from agents import Agent, Runner
from agents.tool import WebSearchTool, function_tool
from tensorlake.applications import application, function, run_local_application, Image

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

#### Running locally

The complete application code is available at [examples/readme_example/city_guide.py](examples/readme_example/city_guide.py).
The following code is included to run it locally on your computer:

```python
if __name__ == "__main__":
    CITY = "Paris"
    
    print(f"Generating city guide for: {CITY}\n")
    
    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        exit(1)

    # Run locally using Tensorlake's local runner
    request = run_local_application("city_guide_app", CITY)
    response = request.output()
    
    print("\n" + "="*50)
    print("CITY GUIDE")
    print("="*50 + "\n")
    print(response)
```

Run the application locally:

```bash
python examples/readme_example/city_guide.py
```

The application will orchestrate multiple OpenAI Agents with tool calls to generate a personalized city guide. Each agent runs in its own sandbox and can execute code (like temperature conversion) and make web searches.

Here is some example output from the simplified version:

```bash
==================================================
CITY GUIDE
==================================================

Welcome to Paris! Today, the weather is cloudy with a current temperature of about 8°C. As you explore the city, you can expect evening and nighttime temperatures to stay between 5°C and 6°C.

Don’t forget your jacket as you stroll along the Seine or visit the Eiffel Tower! Paris can feel especially charming under a cloudy sky, so embrace the cozy atmosphere and maybe stop by a café for a warm drink.

If you need tips for what to do on a cloudy day in Paris, just let me know—enjoy your stay!
```

Testing your applications locally is convenient during development. There's no need to wait until the application is deployed to see how it works.

#### Deploying and running on Tensorlake Cloud

To run the application on Tensorlake Cloud, it first needs to be deployed.

1. Set `TENSORLAKE_API_KEY` environment variable in your shell session:
```bash
export TENSORLAKE_API_KEY="Paste your API key here"
```
2. Set `OPENAI_API_KEY` environment variable in your Tensorlake Secrets so that your application can make calls to OpenAI:
```bash
tensorlake secrets set OPENAI_API_KEY "Paste your API key here"
```
3. Deploy the application to Tensorlake Cloud:
```bash
tensorlake deploy examples/readme_example/city_guide.py
```
4. Run the remote test script, found in `examples/readme_example/test_remote_app.py`:
```python
from tensorlake.applications import run_remote_application

city = "San Francisco"

# Run the application remotely
request = run_remote_application("city_guide_app", city)
print(f"Request ID: {request.id}")

# Get the output
response = request.output()
print(response)
```

5. The application will execute on Tensorlake Cloud, with each function running in its own isolated sandbox.

### Updating your application
Any time you update your application, just re-deploy it to Tensorlake Cloud:
```bash
tensorlake deploy examples/readme_example/city_guide.py
```

And run the remote test script again:
```bash
python examples/readme_example/test_remote_app.py
```

## Learn more about Tensorlake's Agentic Applications 

* [Agentic Applications Documentation](https://docs.tensorlake.ai/applications/quickstart)
* [Key programming concepts in Tensorlake Agentic Applications](https://docs.tensorlake.ai/applications/compute)
* [Dependencies and container images in Tensorlake Agentic Applications](https://docs.tensorlake.ai/applications/images)
* [Open Source Compute Engine](https://docs.tensorlake.ai/opensource/indexify)
