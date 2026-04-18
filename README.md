<h1 align="center">
    <img width="1100" height="259" alt="Group 39884" src="https://github.com/user-attachments/assets/ac9adfc2-53cb-427e-ad6a-91394cdee961" />

</h1>

<p align="center">Build agents with sandboxes and serverless orchestration runtime</p>
<div align="center">


[![PyPI Version](https://img.shields.io/pypi/v/tensorlake)](https://pypi.org/project/tensorlake/)
[![Python Support](https://img.shields.io/pypi/pyversions/tensorlake)](https://pypi.org/project/tensorlake/)
[![License](https://img.shields.io/github/license/tensorlakeai/tensorlake)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-tensorlake.ai-blue)](https://docs.tensorlake.ai)
[![Slack](https://img.shields.io/badge/slack-TensorlakeCloud-purple?logo=slack)](https://join.slack.com/t/tensorlakecloud/shared_invite/zt-32fq4nmib-gO0OM5RIar3zLOBm~ZGqKg)

</div>

Tensorlake is a compute infrastructure platform for building agentic applications with sandboxes. 

The Sandbox API creates MicroVM sandboxes which you can use to run agents, or use them as an isolated environment for running tools or LLM generated code.

In addition to stateful VMs, you can also add long running orchestration capabilites to Agents using a serverless funtion runtime with fan-out capabilities.

## Sandboxes

Tensorlake Sandboxes are stateful Firecracker MicroVMs built for instant, stateful execution environments for AI agents — spin up millions of VMs with near-SSD filesystem performance.

### Key capabilities
* **Fastest Filesystem I/O** — Block-based storage achieving near-SSD speeds inside virtual machines. In SQLite benchmarks (2 vCPUs, 4 GB RAM), Tensorlake completes in **2.45s** vs Vercel 3.00s (1.2×), E2B 3.92s (1.6×), Modal 4.66s (1.9×), and Daytona 5.51s (2.2×).
* **Fast startup** — Sandboxes created in under a second via Lattice, a dynamic cluster scheduler.
* **Snapshots & cloning** — Snapshot at any point to create durable memory and filesystem checkpoints; clone running sandboxes instantaneously across machines.
* **Auto suspend/resume** — Sandboxes suspend when idle and resume in under a second without losing any memory or filesystem state.
* **Live migration** — Sandboxes automatically move between machines during updates with only a brief pause of a few seconds.
* **Scale** — Supports up to 5 million sandboxes in a single project.

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
tensorlake sbx create --image tensorlake/tensorlake/ubuntu-minimal

# Run a command inside it
tensorlake sbx exec <sandbox-id> -- sh -lc "printf 'Hello from the sandbox!\n'"

# Copy a file into the sandbox
tensorlake sbx cp ./my_script.py <sandbox-id>:/tmp/my_script.py

# Open an interactive terminal
tensorlake sbx ssh <sandbox-id>

# Terminate when done
tensorlake sbx terminate <sandbox-id>
```

`--image` expects a sandbox image name such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image name, not an arbitrary Docker image reference.

### Create a Sandbox Programmatically

```python
from tensorlake.sandbox import SandboxClient

client = SandboxClient.for_cloud(api_key="your-api-key")

# Create a sandbox and connect to it
with client.create_and_connect(image="tensorlake/ubuntu-minimal") as sandbox:
    # Run a command
    result = sandbox.run("sh", ["-lc", "printf 'Hello from the sandbox!\\n'"])
    print(result.stdout)  # "Hello from the sandbox!"

    # Write and read files
    sandbox.write_file("/tmp/data.txt", b"some data")
    content = sandbox.read_file("/tmp/data.txt")

    # Start a long-running process
    proc = sandbox.start_process("sleep", ["300"])
    print(proc.pid)

# Sandbox is automatically terminated when the context manager exits
```

`create_and_connect()` and TypeScript `createAndConnect()` wait for the sandbox
to reach `running`. On newer servers, a sandbox can also become `suspended`
during startup when a timeout fires. The SDKs surface `suspended` as a startup
failure when they observe it via `get()`, but some server versions still fold
that path into a generic startup timeout on the blocking create-and-wait
endpoint. If you need to distinguish `suspended` from a true startup timeout,
use `create()` and poll `get()` yourself.

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
    image="tensorlake/ubuntu-minimal",
    warm_containers=3,
)

# Claim a sandbox instantly from the pool
resp = client.claim(pool.pool_id)
sandbox = client.connect(resp.sandbox_id)

# Named sandboxes can be reconnected later by name
named = client.create(image="tensorlake/ubuntu-minimal", name="stable-name")
sandbox = client.connect("stable-name")
```

---

## Orchestrate

Create orchestration APIs on a distributed runtime with automatic scaling, fan-out capabilities and built-in tracking. The orchestration APIs can be invoked using HTTP requests or using the Python SDK.

### Quickstart

Decorate your entrypoint with `@application()` and functions with `@function()`. Each function runs in its own isolated sandbox.

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

#### Deploy to Tensorlake

1. Set your API keys:
```bash
export TENSORLAKE_API_KEY="your-api-key"
tl secrets set OPENAI_API_KEY "your-openai-key"
```

2. Deploy:
```bash
tl deploy examples/readme_example/city_guide.py
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

```

---

## Learn More

* [Sandbox Documentation](https://docs.tensorlake.ai/sandboxes/introduction)
* [Orchestrate Documentation](https://docs.tensorlake.ai/applications/quickstart)
