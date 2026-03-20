<h1 align="center">
    <img width="1100" height="259" alt="Group 39884" src="https://github.com/user-attachments/assets/ac9adfc2-53cb-427e-ad6a-91394cdee961" />

</h1>

<p align="center">Secure cloud sandboxes and durable serverless applications for AI agents</p>
<div align="center">


[![PyPI Version](https://img.shields.io/pypi/v/tensorlake)](https://pypi.org/project/tensorlake/)
[![Python Support](https://img.shields.io/pypi/pyversions/tensorlake)](https://pypi.org/project/tensorlake/)
[![License](https://img.shields.io/github/license/tensorlakeai/tensorlake)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-tensorlake.ai-blue)](https://docs.tensorlake.ai)
[![Slack](https://img.shields.io/badge/slack-TensorlakeCloud-purple?logo=slack)](https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg)

</div>

## Products

- **[Sandboxes](#sandboxes)** — Secure, isolated cloud environments for running code. Spin up a sandbox in seconds, execute commands, transfer files, and manage processes — from the CLI or Python SDK.

- **[Applications](#applications)** — Deploy durable, serverless agentic applications and workflows with automatic scaling and fault tolerance.

---

## Sandboxes

Sandboxes are secure, isolated cloud environments for running arbitrary code. Each sandbox is a lightweight container with its own filesystem, network, and process space. Use them to give your AI agents a safe place to execute code, run tools, or interact with the outside world.

### Installation

```bash
pip install tensorlake
```

### Setup

Sign up at [cloud.tensorlake.ai](https://cloud.tensorlake.ai/) and get your API key.

```bash
export TENSORLAKE_API_KEY="your-api-key"
tensorlake login
```

### Create Your First Sandbox (CLI)

Create a sandbox, run a command, and clean up:

```bash
# Create a sandbox
tensorlake sbx create --image python:3.11-slim

# Run a command inside it
tensorlake sbx exec <sandbox-id> -- python -c "print('Hello from the sandbox!')"

# Copy a file into the sandbox
tensorlake sbx cp ./my_script.py <sandbox-id>:/tmp/my_script.py

# Open an interactive terminal
tensorlake sbx ssh <sandbox-id>

# Terminate when done
tensorlake sbx terminate <sandbox-id>
```

### Create a Sandbox Programmatically (Python SDK)

```python
from tensorlake.sandbox import SandboxClient

client = SandboxClient.for_cloud(api_key="your-api-key")

# Create a sandbox and connect to it
with client.create_and_connect(image="python:3.11-slim") as sandbox:
    # Run a command
    result = sandbox.run("python", ["-c", "print('Hello from the sandbox!')"])
    print(result.stdout)  # "Hello from the sandbox!"

    # Write and read files
    sandbox.write_file("/tmp/data.txt", b"some data")
    content = sandbox.read_file("/tmp/data.txt")

    # Start a long-running process
    proc = sandbox.start_process("python", ["-m", "http.server", "8080"])
    print(proc.pid)

# Sandbox is automatically terminated when the context manager exits
```

### Snapshots

Save the state of a sandbox and restore it later:

```python
# Snapshot a running sandbox
snapshot = client.snapshot_and_wait(sandbox_id)

# Later, create a new sandbox from the snapshot
with client.create_and_connect(snapshot_id=snapshot.snapshot_id) as sandbox:
    # Picks up right where you left off
    result = sandbox.run("ls", ["/tmp"])
    print(result.stdout)
```

### Sandbox Pools

Pre-warm containers for fast startup:

```python
# Create a pool with warm containers
pool = client.create_pool(
    image="python:3.11-slim",
    warm_containers=3,
)

# Claim a sandbox instantly from the pool
resp = client.claim(pool.pool_id)
sandbox = client.connect(resp.sandbox_id)
```

---

## Applications

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

---

## Learn More

* [Sandbox Documentation](https://docs.tensorlake.ai)
* [Applications Documentation](https://docs.tensorlake.ai/applications/quickstart)
* [Programming Concepts](https://docs.tensorlake.ai/applications/compute)
* [Dependencies & Images](https://docs.tensorlake.ai/applications/images)
* [Open Source Compute Engine](https://docs.tensorlake.ai/opensource/indexify)
