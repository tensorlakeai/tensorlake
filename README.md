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

Tensorlake is the platform for agentic applications. Build and deploy scalable, durable, data-heavy applications in minutes, leveraging our best-in-class Document AI API for transforming unstructured documents into AI-ready data.

![Animation showing the Tensorlake Document Ingestion UI parsing an ACORD doc into Markdown](/assets/README-DocAI.gif)
</div>

## Features

- **[Document Ingestion](#document-ingestion-quickstart)** - Parse documents (PDFs, DOCX, spreadsheets, presentations, images, and raw text) to markdown or extract structured data with schemas. This is powered by Tensorlake's state of the art layout detection and table recognition models. Review our [benchmarks here](https://www.tensorlake.ai/blog/benchmarks).

- **[Agentic Applications](#build-durable-agentic-applications-in-python)** - Build and deploy data ingestion and orchestration APIs using durable functions in Python that scales automatically on fully managed infrastructure.

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

Tensorlake Agentic Applications allow you to chain Python functions into durable, distributed workflows. These functions can be deployed to a serverless backend with a single command.

**No Queues**: We handle the state and orchestration.

**Zero Infra**: Write Python, deploy to Tensorlake.

### Agentic Applications Quickstart

Define an Application by implementing its data transformation steps as Python functions decorated with `@function()`. Specify the image needed for each function and any required secrets as parameters to the `@function()` decorator. Add the `@application()` decorator to each entrypoint function. 

The example below creates a workflow with the following steps:

1. Use OpenAI to analyze the sentiment of customer feedback.
2. Use OpenAI to draft a customer support email based on the sentiment.

```python
import os
from typing import Dict
from openai import OpenAI
from tensorlake.applications import application, function, run_local_application, Image

image = (
    Image(base_image="python:3.11-slim", name="openai_story_writer")
    .run("pip install openai")
)

@function(
    description="Analyzes the sentiment of input text",
    secrets=["OPENAI_API_KEY"],
    image=image
)
def analyze_sentiment(feedback: str) -> str:
    """Step 1: Analyze the sentiment of the input text."""
    print(f"Analyzing: {feedback}")
    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a sentiment analyzer. Respond with only one word: POSITIVE or NEGATIVE."},
            {"role": "user", "content": feedback}
        ]
    )
    return response.choices[0].message.content.strip()

@function(
    description="Drafts a customer support email based on the sentiment.",
    secrets=["OPENAI_API_KEY"],
    image=image
)
def draft_response(sentiment: str) -> str:
    """Step 2: Draft a customer support email based on the sentiment."""
    print(f"Drafting email for {sentiment} feedback...")
    if sentiment == "NEGATIVE":
        prompt = "Write a short, empathetic apology email to a customer."
    else:
        prompt = "Write a short, enthusiastic thank you email to a customer."
    
    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

@application(
    tags={"type": "quickstart", "use_case": "customer_support"},
)
@function(
    description="Customer Support application.",
    image=image
)
def customer_support(feedbacks: Dict[str, str]) -> Dict[str, str]:
    """
    Main application workflow:
    1. Analyze sentiment for all feedbacks in parallel.
    2. Draft responses for all sentiments in parallel.
    3. Aggregate results into a dictionary.
    """
    names = list(feedbacks.keys())
    feedback_texts = list(feedbacks.values())
    
    # 1. Analyze sentiment in parallel
    sentiments = analyze_sentiment.map(feedback_texts)
    
    # 2. Draft responses in parallel
    responses = draft_response.map(sentiments)
    
    # 3. Compile: Return dictionary
    return dict(zip(names, responses))
```

#### Running locally

The application code is available at [examples/readme_example/customer_support_example.py](examples/readme_example/customer_support_example.py).
The following code was added there to create the workflow and run it locally on your computer:

```python
if __name__ == "__main__":
    # Example usage
    FEEDBACKS = {
        "customer_a": "The product was great!",
        "customer_b": "I am very disappointed with the service.",
        "customer_c": "It worked perfectly, thank you!",
    }
    
    print(f"Generating responses for: {FEEDBACKS}\n")
    
    if not os.environ.get("OPENAI_API_KEY"):
        print("Error: OPENAI_API_KEY environment variable is not set.")
        exit(1)

    # Run locally using Tensorlake's local runner
    request = run_local_application(customer_support, FEEDBACKS)
    response = request.output()
    
    print("\n" + "="*50)
    print("FINAL RESPONSE")
    print("="*50 + "\n")
    print(response)
```

Run the workflow locally:

```bash
python examples/readme_example/customer_support_example.py
```

In the console output you can see the result of the application:
```json
{
  "customer_a": "Subject: Thank You for Choosing Us!\n\nDear [Customer's Name],\n\nI hope this message finds you well. I just wanted to take a moment to express our heartfelt thanks for choosing [Your Company Name]! We are thrilled to have the opportunity to serve you and are grateful for your trust in us.\n\nYour satisfaction is our top priority, and we are committed to going above and beyond to ensure you have the best experience possible. If there is anything more we can do for you, please don't hesitate to let us know.\n\nThank you once again for your support. We look forward to serving you again soon!\n\nWarm regards,\n\n[Your Name]  \n[Your Position]  \n[Your Company Name]  \n[Contact Information]",
  "customer_b": "Subject: Sincere Apologies for Your Recent Experience\n\nDear [Customer's Name],\n\nI hope this message finds you well. I am writing to personally apologize for the inconvenience you experienced with our product/service. Ensuring our customers have a positive experience is our top priority, and I am truly sorry that we fell short of your expectations on this occasion.\n\nPlease rest assured that we are taking your feedback seriously and are working diligently to address the issues you've encountered. Your satisfaction is important to us, and we are committed to making this right for you.\n\nIf there's anything specific we can do to improve your experience, please do not hesitate to let us know. We value your trust and are eager to resolve this matter to your satisfaction.\n\nThank you for your understanding and patience. We truly appreciate your business and look forward to serving you better in the future.\n\nWarm regards,\n\n[Your Name]  \n[Your Position]  \n[Your Company]  \n[Contact Information]  ",
  "customer_c": "Subject: Thank You for Your Purchase!\n\nHi [Customer's Name],\n\nI hope this message finds you well. I just wanted to take a moment to personally thank you for choosing us. Your support means the world to us, and we are thrilled to have you as a part of our community.\n\nWe strive to provide the best products and services, and your satisfaction is our top priority. If you have any questions or need assistance, please don't hesitate to reach out.\n\nOnce again, thank you for your trust in us. We're excited to serve you and look forward to seeing you again soon!\n\nWarm regards,\n\n[Your Name]  \n[Your Position]  \n[Company Name]  \n[Contact Information]"
}
```

Testing your applications locally is convenient during its development. There's no need to wait until the application is deployed to see how it works.

#### Deploying and running on Tensorlake Cloud

To run the application on tensorlake cloud it first needs to get deployed there.

1. Set `TENSORLAKE_API_KEY` environment variable in your shell session:
```bash
export TENSORLAKE_API_KEY="Paste your API key here"
```
2. Set `OPENAI_API_KEY` environment variable in your Tensorlake Secrets so that your appluication can make calls to OpenAI:
```bash
tensorlake secrets set OPENAI_API_KEY "Paste your API key here"
```
3. Deploy the application to Tensorlake Cloud:
```bash
tensorlake deploy examples/readme_example/customer_support_example.py
```
4. Run the remote test script, found in `examples/readme_example/test_remote_app.py`:
```python
from tensorlake.applications import run_remote_application
from readme_example import customer_support

# ... (feedbacks dictionary definition)

# Run the application remotely
request = run_remote_application(customer_support, feedbacks)
print(f"Request ID: {request.id}")

# Get the output
response = request.output()
print(response)
```

```bash
python examples/readme_example/test_remote_app.py
```

5. Confirm you get the same response as before, maybe slightly different since the emails are generated by OpenAI.

### Updating your application
Any time you update your application, just re-deploy it to Tensorlake Cloud:
```bash
tensorlake deploy examples/readme_example/customer_support_example.py
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
