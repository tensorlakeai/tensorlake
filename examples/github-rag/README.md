# GitHub RAG Application

A Tensorlake application that extracts code from GitHub repositories and stores them in a vector database (LanceDB) for semantic search.

## Overview

This application:
- Fetches code files from a GitHub repository
- Creates embeddings using OpenAI's embedding models
- Stores the embeddings in LanceDB for semantic search
- Provides a RAG (Retrieval-Augmented Generation) interface for code search

## Architecture

The application is built using Tensorlake's serverless data application platform with:
- **`@application()`** decorator: Defines the main application entry point
- **`@cls()`** decorator: Defines the processor class with initialization logic
- **`@function()`** decorator: Defines individual functions in the application that will run in isolated environments
- **Secrets Management**: Securely manages GitHub, OpenAI API, and LanceDB API tokens

## Prerequisites

- Python 3.12+
- GitHub Personal Access Token
- OpenAI API Key
- LanceDB Cloud API Key
- Tensorlake CLI (for deployment)

## Supported file formats

The application extracts code files with the following extensions:
- Python (`.py`)
- JavaScript/TypeScript (`.js`, `.ts`)
- Java (`.java`)
- Go (`.go`)
- Rust (`.rs`)
- C/C++ (`.c`, `.cpp`, `.h`, `.hpp`)
- C# (`.cs`)
- Ruby (`.rb`)
- PHP (`.php`)
- Swift (`.swift`)
- Kotlin (`.kt`)
- Markdown (`.md`)
- Configuration files (`.json`, `.yaml`, `.yml`, `.toml`, `.xml`)
- Text files (`.txt`)

## Installation

```bash
# Install dependencies
uv sync

# Or with pip
pip install -e .
```

## Configuration

### Set Up Secrets

#### For Local Development

```bash
export GITHUB_TOKEN="your_github_token"
export OPENAI_API_KEY="your_openai_api_key"
export LANCEDB_API_KEY="your_lancedb_api_key"
```

#### For Tensorlake Deployment

```bash
tensorlake secrets set GITHUB_TOKEN your_token
tensorlake secrets set OPENAI_API_KEY your_key
tensorlake secrets set LANCEDB_API_KEY your_lancedb_key
```

## Usage

### Option 1: Local Testing

Process a single repository locally:

```bash
# Use default repository (tensorlakeai/tensorlake)
python main.py

# Or specify a different repository
export GITHUB_REPO="owner/repo-name"
export MAX_FILES=50
python main.py
```

### Option 2: Deploy to Tensorlake

1. **Deploy the application:**

```bash
tensorlake deploy main.py
```

2. **Invoke via API:**

```bash
curl -X POST https://api.tensorlake.com/applications/store_github_code \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "repo_path": "tensorlakeai/tensorlake",
    "max_files": 100
  }'
```

3. **Or via Python:**

```python
from tensorlake.applications import run_application
from main import store_github_code, RepositoryInput

input_data = RepositoryInput(
    repo_path="tensorlakeai/tensorlake",
    max_files=50
)

result = run_application(store_github_code, input_data, remote=True)
print(result.output())
```

## Search the Database

After processing repositories, you can search the vector database using the `search_github_code` application.

### Search via Python (Recommended)

```python
from tensorlake.applications import run_application
from main import search_github_code, SearchInput

# Create search input
search_input = SearchInput(
    query="How to implement authentication?",
    limit=5
)

# Run search locally
result = run_application(search_github_code, search_input, remote=False)
search_results = result.output()

# Display results
for row in search_results.iter_rows(named=True):
    print(f"File: {row['file_path']}")
    print(f"Repo: {row['repo_name']}")
    print(f"URL: {row['url']}")
    print()
```

### Search via API

```bash
curl -X POST https://api.tensorlake.com/applications/search_github_code \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d '{
    "query": "How to implement authentication?",
    "limit": 5
  }'
```

## Examples

### Example 1: Process Multiple Repositories

```python
from tensorlake.applications import run_application
from main import store_github_code, RepositoryInput

repositories = [
    "tensorlakeai/tensorlake",
    "langchain-ai/langchain",
    "openai/openai-python"
]

for repo in repositories:
    input_data = RepositoryInput(
        repo_path=repo,
        max_files=50
    )
    result = run_application(store_github_code, input_data, remote=False)
    output = result.output()
    print(f"{repo}: {output.status} - {output.message}")
```

### Example 2: Search and Filter Results

```python
from tensorlake.applications import run_application
from main import search_github_code, SearchInput
import polars as pl

# Search for authentication code
search_input = SearchInput(query="authentication", limit=10)
result = run_application(search_github_code, search_input, remote=False)
results = result.output()

# Filter by repository
filtered = results.filter(pl.col('repo_name').str.contains("tensorlake"))

# Display file paths
print(filtered.select(['repo_name', 'file_path']).to_pandas())
```

### Example 3: End-to-End Workflow

```python
from tensorlake.applications import run_application
from main import store_github_code, search_github_code, RepositoryInput, SearchInput

# Step 1: Process repository
store_input = RepositoryInput(
    repo_path="tensorlakeai/tensorlake",
    max_files=50
)
store_result = run_application(store_github_code, store_input, remote=False)
print(f"Storage: {store_result.output().status}")

# Step 2: Search the database
search_input = SearchInput(
    query="vector database implementation",
    limit=5
)
search_result = run_application(search_github_code, search_input, remote=False)
results = search_result.output()

# Step 3: Display results
for idx, row in enumerate(results.iter_rows(named=True), 1):
    print(f"\n{idx}. {row['file_path']}")
    print(f"   Repo: {row['repo_name']}")
    print(f"   URL: {row['url']}")
```

## Input Format

### Store GitHub Code

The `store_github_code` application accepts JSON with these fields:

```json
{
  "repo_path": "owner/repo-name",     // Required
  "max_files": 100,                    // Optional, default: 100
  "embedding_model": "text-embedding-3-small"  // Optional
}
```

### Search GitHub Code

The `search_github_code` application accepts JSON with these fields:

```json
{
  "query": "search query text",       // Required
  "limit": 10                          // Optional, default: 10
}
```

## Output Format

Returns JSON with processing results:

```json
{
  "repo_name": "owner/repo-name",
  "files_processed": 85,
  "embeddings_created": 85,
  "status": "success",
  "message": "Successfully processed 85 code files"
}
```

## Troubleshooting

### "GITHUB_TOKEN secret must be set"

```bash
# Make sure you've set the secret
export GITHUB_TOKEN="your_token"

# Or for Tensorlake
tensorlake secrets set GITHUB_TOKEN your_token
```

### "LANCEDB_API_KEY secret must be set"

Get your LanceDB Cloud API key from [LanceDB Cloud](https://cloud.lancedb.com):

```bash
# For local development
export LANCEDB_API_KEY="your_lancedb_api_key"

# Or for Tensorlake
tensorlake secrets set LANCEDB_API_KEY your_lancedb_key
```

### "No results found"

Make sure you've processed at least one repository:

```bash
python main.py
```

### "Repository not found"

Verify the repository path format is correct: `owner/repo-name`

## Support

- [Tensorlake Documentation](https://docs.tensorlake.ai)
- [Tensorlake Slack](https://tensorlake.ai/slack)
- [GitHub Issues](https://github.com/tensorlakeai/tensorlake/issues)
