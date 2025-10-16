"""
GitHub RAG Application using LanceDB
Extracts code from a GitHub repository and stores it in a vector database for semantic search.
"""

import os
import logging
from typing import Any

import github3
import polars as pl
from openai import OpenAI
import lancedb
from tensorlake.applications import application, function, cls, Image, run_application
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Define the image with required dependencies
processor_image = Image(name="github_rag_processor").run(
    "pip install github3.py polars openai lancedb"
)


class RepositoryInput(BaseModel):
    """Input model for the GitHub RAG application."""

    repo_path: str  # e.g., "tensorlakeai/tensorlake"
    max_files: int = 100
    embedding_model: str = "text-embedding-3-small"


class ProcessingResult(BaseModel):
    """Output model for the GitHub RAG application."""

    repo_name: str
    files_processed: int
    embeddings_created: int
    status: str
    message: str


class SearchInput(BaseModel):
    """Search model for the GitHub RAG application."""

    query: str
    limit: int = 10


@cls()
class GitHubRAGProcessor:
    """Processes GitHub repositories and stores them in a RAG database."""

    def __init__(self):
        """
        Initialize the GitHub RAG processor.
        Secrets are loaded from environment variables.
        """
        # Load secrets from environment variables
        # These will be available after declaring them in the @function decorator
        self.github_token = os.environ.get("GITHUB_TOKEN")
        self.openai_api_key = os.environ.get("OPENAI_API_KEY")
        self.lancedb_api_key = os.environ.get("LANCEDB_API_KEY")
        lancedb_uri = os.environ.get("LANCEDB_URI")
        lancedb_region = os.environ.get("LANCEDB_REGION", "us-east-1")

        if not self.github_token:
            raise ValueError("GITHUB_TOKEN secret must be set")
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY secret must be set")
        if not self.lancedb_api_key:
            raise ValueError("LANCEDB_API_KEY secret must be set")
        if not lancedb_uri:
            raise ValueError("LANCEDB_URI secret must be set")

        # Initialize clients (will be created per request)
        self.github = github3.login(token=self.github_token)
        self.openai = OpenAI(api_key=self.openai_api_key)
        self.lancedb = lancedb.connect(
            uri=lancedb_uri, api_key=self.lancedb_api_key, region=lancedb_region
        )
        self.embedding_model = "text-embedding-3-small"

        logger.info("Initialized GitHub RAG Processor")

    def get_repository(self, repo_path: str) -> Any:
        """
        Fetch a single GitHub repository by its path.

        Args:
            repo_path: Path to the repository (e.g., "tensorlakeai/tensorlake")

        Returns:
            Repository object
        """
        logger.info(f"Fetching repository: {repo_path}")
        owner, repo_name = repo_path.split("/")
        repo = self.github.repository(owner, repo_name)
        if not repo:
            raise ValueError(f"Repository {repo_path} not found")
        logger.info(f"Found repository: {repo.full_name}")
        return repo

    def extract_code_files(
        self, repo: Any, max_files: int = 100
    ) -> list[dict[str, Any]]:
        """
        Extract code files from a repository.

        Args:
            repo: GitHub repository object
            max_files: Maximum number of files to extract

        Returns:
            List of dictionaries containing file information
        """
        logger.info(f"Extracting code from repository: {repo.name}")
        files_data = []

        try:
            # Get the default branch
            default_branch = repo.default_branch

            # Get repository contents recursively
            contents = repo.directory_contents("", return_as=dict, ref=default_branch)
            file_count = 0

            def process_contents(contents_dict, path=""):
                nonlocal file_count
                if file_count >= max_files:
                    return

                for name, item in contents_dict.items():
                    if file_count >= max_files:
                        break

                    if item.type == "file":
                        # Skip large files and binary files
                        if item.size > 100000:  # Skip files larger than 100KB
                            continue

                        # Only process text files
                        text_extensions = {
                            ".py",
                            ".js",
                            ".ts",
                            ".java",
                            ".go",
                            ".rs",
                            ".cpp",
                            ".c",
                            ".h",
                            ".hpp",
                            ".cs",
                            ".rb",
                            ".php",
                            ".swift",
                            ".kt",
                            ".md",
                            ".txt",
                            ".json",
                            ".yaml",
                            ".yml",
                            ".toml",
                            ".xml",
                        }

                        if not any(name.endswith(ext) for ext in text_extensions):
                            continue

                        try:
                            # Get file content
                            content = (
                                item.decoded.decode("utf-8")
                                if hasattr(item, "decoded")
                                else ""
                            )

                            files_data.append(
                                {
                                    "repo_name": repo.full_name,
                                    "file_path": item.path,
                                    "file_name": name,
                                    "content": content,
                                    "size": item.size,
                                    "type": "code",
                                    "url": item.html_url,
                                }
                            )
                            file_count += 1

                        except Exception as e:
                            logger.warning(f"Error reading file {item.path}: {e}")

                    elif item.type == "dir":
                        # Recursively process directories
                        try:
                            sub_contents = repo.directory_contents(
                                item.path, return_as=dict, ref=default_branch
                            )
                            process_contents(sub_contents, item.path)
                        except Exception as e:
                            logger.warning(
                                f"Error accessing directory {item.path}: {e}"
                            )

            process_contents(contents)
            logger.info(f"Extracted {len(files_data)} code files from {repo.name}")

        except Exception as e:
            logger.error(f"Error extracting code from {repo.name}: {e}")

        return files_data

    def create_embeddings(
        self, texts: list[str], batch_size: int = 100
    ) -> list[list[float]]:
        """
        Create embeddings using OpenAI API.

        Args:
            texts: List of texts to embed
            batch_size: Number of texts to embed in each batch

        Returns:
            List of embedding vectors
        """
        logger.info(f"Creating embeddings for {len(texts)} texts")
        embeddings = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]

            try:
                response = self.openai.embeddings.create(
                    model=self.embedding_model, input=batch
                )

                batch_embeddings = [item.embedding for item in response.data]
                embeddings.extend(batch_embeddings)

                logger.info(
                    f"Created embeddings for batch {i // batch_size + 1}/{(len(texts) - 1) // batch_size + 1}"
                )

            except Exception as e:
                logger.error(
                    f"Error creating embeddings for batch {i // batch_size + 1}: {e}"
                )
                # Add zero vectors as placeholders
                embedding_dim = 1536 if "small" in self.embedding_model else 3072
                embeddings.extend([[0.0] * embedding_dim for _ in batch])

        return embeddings

    def store_in_lancedb(
        self,
        data: list[dict[str, Any]],
        table_name: str = "code",
    ):
        """
        Store data in LanceDB with embeddings.

        Args:
            data: List of dictionaries containing code file data to store
            lancedb_uri: URI for LanceDB database
            table_name: Name of the LanceDB table
        """
        if not data:
            logger.warning("No data to store")
            return

        logger.info(f"Storing {len(data)} items in LanceDB table '{table_name}'")

        # Prepare texts for embedding - all items are code files
        texts = []
        for item in data:
            # For code files, combine file path and content
            text = f"File: {item['file_path']}\n\n{item['content']}"
            texts.append(text)

        # Create embeddings
        embeddings = self.create_embeddings(texts)

        # Prepare data for LanceDB
        records = []
        for item, embedding, text in zip(data, embeddings, texts):
            record = {
                "vector": embedding,
                "text": text,
                "type": "code",
                "repo_name": item["repo_name"],
                "file_path": item.get("file_path", ""),
                "file_name": item.get("file_name", ""),
                "url": item.get("url", ""),
                "metadata": str(item),  # Store full metadata as string
            }
            records.append(record)

        # Create or append to LanceDB table
        try:
            # Try to get existing table
            table = self.lancedb.open_table(table_name)
            table.add(records)
            logger.info(
                f"Added {len(records)} records to existing table '{table_name}'"
            )
        except Exception:
            # Create new table if it doesn't exist
            table = self.lancedb.create_table(table_name, records)
            logger.info(f"Created new table '{table_name}' with {len(records)} records")

    @function(
        description="Process a GitHub repository and store code in vector database",
    )
    def process_repository(self, input_data: RepositoryInput) -> ProcessingResult:
        """
        Process a single repository: extract code files and store in LanceDB.

        Args:
            input_data: Repository input configuration

        Returns:
            Processing result with statistics
        """
        try:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"Processing repository: {input_data.repo_path}")
            logger.info(f"{'=' * 60}\n")

            # Get the repository
            repo = self.get_repository(input_data.repo_path)

            # Extract code files
            code_data = self.extract_code_files(repo, max_files=input_data.max_files)

            if not code_data:
                return ProcessingResult(
                    repo_name=input_data.repo_path,
                    files_processed=0,
                    embeddings_created=0,
                    status="warning",
                    message="No code files extracted from repository",
                )

            # Store in LanceDB
            self.store_in_lancedb(code_data, input_data.lancedb_uri)

            logger.info(
                f"Successfully processed and stored {len(code_data)} files from {input_data.repo_path}"
            )

            return ProcessingResult(
                repo_name=input_data.repo_path,
                files_processed=len(code_data),
                embeddings_created=len(code_data),
                status="success",
                message=f"Successfully processed {len(code_data)} code files",
            )

        except Exception as e:
            logger.error(f"Failed to process repository {input_data.repo_path}: {e}")
            return ProcessingResult(
                repo_name=input_data.repo_path,
                files_processed=0,
                embeddings_created=0,
                status="error",
                message=f"Error: {str(e)}",
            )

    def search(
        self, query: str, limit: int = 10, table_name: str = "code"
    ) -> pl.DataFrame:
        """
        Search the RAG database using semantic search.

        Args:
            query: Search query
            table_name: Name of the LanceDB table
            limit: Maximum number of results to return

        Returns:
            Polars DataFrame with search results
        """
        logger.info(f"Searching for: {query}")

        # Create embedding for the query
        query_embedding = self.create_embeddings([query])[0]

        # Search in LanceDB
        table = self.lancedb.open_table(table_name)
        results = table.search(query_embedding).limit(limit).to_pandas()

        # Convert to Polars for better data manipulation
        return pl.from_pandas(results)


@application()
@function(
    image=processor_image,
    description="GitHub RAG Application - Extract code from repositories and store in vector database",
    secrets=[
        "GITHUB_TOKEN",
        "OPENAI_API_KEY",
        "LANCEDB_API_KEY",
        "LANCEDB_URI",
        "LANCEDB_REGION",
    ],
)
def store_github_code(input_data: RepositoryInput) -> ProcessingResult:
    """
    Main application entry point for GitHub RAG.

    Args:
        input_data: Repository input configuration

    Returns:
        Processing result with statistics
    """
    # Create processor instance and process the repository
    processor = GitHubRAGProcessor()
    return processor.process_repository(input_data)


@application()
@function(
    image=processor_image,
    description="GitHub RAG Application - Search code in vector database",
    secrets=[
        "GITHUB_TOKEN",
        "OPENAI_API_KEY",
        "LANCEDB_API_KEY",
        "LANCEDB_URI",
        "LANCEDB_REGION",
    ],
)
def search_github_code(input_data: SearchInput) -> ProcessingResult:
    """
    Main application entry point for GitHub RAG.

    Args:
        input_data: Repository input configuration

    Returns:
        Processing result with statistics
    """
    processor = GitHubRAGProcessor()
    return processor.search(input_data)


def main():
    """Main entry point for local testing of the GitHub RAG application."""

    # Configuration
    REPO_PATH = os.getenv(
        "GITHUB_REPO", "tensorlakeai/tensorlake"
    )  # Default repository for testing
    MAX_FILES = int(os.getenv("MAX_FILES", "50"))  # Limit for testing

    RUN_REMOTE = os.getenv("RUN_REMOTE", "false") == "true"

    # Create input
    input_data = RepositoryInput(
        repo_path=REPO_PATH,
        max_files=MAX_FILES,
        embedding_model="text-embedding-3-small",
    )

    # Process repository
    logger.info(f"Processing repository: {REPO_PATH}")
    result = run_application(store_github_code, input_data, remote=RUN_REMOTE)

    logger.info("\n" + "=" * 60)
    logger.info("Processing Result")
    logger.info("=" * 60)
    logger.info(f"Status: {result.status}")
    logger.info(f"Repository: {result.repo_name}")
    logger.info(f"Files Processed: {result.files_processed}")
    logger.info(f"Embeddings Created: {result.embeddings_created}")
    logger.info(f"Message: {result.message}")

    # Example search if processing was successful
    if result.status == "success":
        logger.info("\n" + "=" * 60)
        logger.info("Example Search Query")
        logger.info("=" * 60)

        search_query = "How to implement vector search?"
        input_data = SearchInput(query=search_query, limit=5)
        result = run_application(search_github_code, input_data, remote=RUN_REMOTE)
        results = result.output()

        logger.info(f"\nTop 5 results for '{search_query}':")
        logger.info(
            "\n"
            + results.select(["type", "repo_name", "file_path"]).to_pandas().to_string()
        )


if __name__ == "__main__":
    main()
