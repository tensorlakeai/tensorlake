import base64
import hashlib
import mimetypes
import sys
from enum import Enum
from pathlib import Path
from typing import List, Literal, Optional, Union

import aiofiles
import httpx
from pydantic import BaseModel, Field
from tqdm import tqdm
from tqdm.asyncio import tqdm as async_tqdm

DOC_AI_BASE_URL = "https://api.tensorlake.ai/documents/v1/"

try:
    import magic

    _HAS_MAGIC = True
except ImportError:
    _HAS_MAGIC = False
    print(
        "Warning: `python-magic` (libmagic) is not installed. Falling back to `mimetypes`. Install it with `pip install python-magic` for better MIME detection."
    )


class PageFragmentType(str, Enum):
    """
    Type of a page fragment.
    """

    SECTION_HEADER = "section_header"

    TEXT = "text"
    TABLE = "table"
    FIGURE = "figure"
    FORMULA = "formula"
    FORM = "form"
    KEY_VALUE_REGION = "key_value_region"
    DOCUMENT_INDEX = "document_index"
    LIST_ITEM = "list_item"

    TABLE_CAPTION = "table_caption"
    FIGURE_CAPTION = "figure_caption"
    FORMULA_CAPTION = "formula_caption"


class Text(BaseModel):
    content: str


class Table(BaseModel):
    content: str
    summary: Optional[str] = None


class Figure(BaseModel):
    content: str
    summary: Optional[str] = None


class PageFragment(BaseModel):
    fragment_type: PageFragmentType
    content: Union[Text, Table, Figure]
    reading_order: Optional[int] = None
    page_number: Optional[int] = None
    bbox: Optional[dict[str, float]] = None


class Page(BaseModel):
    """
    Page in a document.
    """

    page_number: int
    page_fragments: Optional[List[PageFragment]] = []
    layout: Optional[dict] = {}


class Document(BaseModel):
    """
    Document in a document.
    """

    pages: List[Page]


class OutputFormat(str, Enum):
    """
    Output format for parsing a document.

    MARKDOWN: The parsed document is returned in Markdown format. Using Markdown requires setting a chunking strategy.
    JSON: The parsed document is returned in JSON format.
    """

    MARKDOWN = "markdown"
    JSON = "json"


class ChunkingStrategy(str, Enum):
    """
    Chunking strategy for parsing a document.

    NONE: No chunking is applied.
    PAGE: The document is chunked by page.
    SECTION_HEADER: The document is chunked by section headers.
    """

    NONE = "none"
    PAGE = "page"
    SECTION_HEADER = "section_header"


class TableParsingStrategy(str, Enum):
    """
    Algorithm to use for parsing tables in a document.

    TSR: Table Structure Recognition. Great for structured tables.
    VLM: Visual Layout Model. Great for unstructured tables or semi-structured tables.
    """

    TSR = "tsr"
    VLM = "vlm"


class TableOutputMode(str, Enum):
    """
    Output mode for tables in a document.

    JSON: The table is returned in JSON format.
    MARKDOWN: The table is returned in Markdown format.
    HTML: The table is returned in HTML format.
    """

    JSON = "json"
    MARKDOWN = "markdown"
    HTML = "html"


class ModelProvider(str, Enum):
    """
    The model provider to use for structured data extraction.

    TENSORLAKE: private models, running on Tensorlake infrastructure.
    SONNET: Claude 3.5 Sonnet model.
    GPT4OMINI: GPT-4o-mini model.
    """

    TENSORLAKE = "tensorlake"
    SONNET = "claude-3-5-sonnet-latest"
    GPT4OMINI = "gpt-4o-mini"


class JobResult(BaseModel):
    job_id: str = Field(alias="jobId")
    file_id: str = Field(alias="fileId")
    job_type: str = Field(alias="jobType")
    chunks: List[str] = Field(alias="chunks", default_factory=list)
    document: Optional[Document] = Field(alias="document", default=None)
    status: Literal["processing", "successful", "failure", "pending"]


class FileUploader:
    """
    Private class for uploading files to DocumentAI.
    """

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.Client(base_url=DOC_AI_BASE_URL, timeout=None)
        self._async_client = httpx.AsyncClient(base_url=DOC_AI_BASE_URL, timeout=None)

    def upload_file(self, file_path: str):
        """
        Upload a file to the Tensorlake

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """

        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        # check if file is longer than 10 mb
        if path.stat().st_size > 10 * 1024 * 1024:
            return self.upload_large_file(path)

        with open(path, "rb") as f:
            files = {"file": (f.name, f)}
            response = self._client.post(
                url="files",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                },
                files=files,
            )
            response.raise_for_status()
            resp = response.json()
            return resp.get("id")

    async def upload_file_async(self, path: Union[str, Path]) -> str:
        """
        Upload a file to the Tensorlake asynchronously.

        Args:
            file_path: Path to the file to upload

        Returns:
            File ID of the uploaded file. This ID can be used to reference the file in other API calls.
            String in the format "tensorlake-<ID>"

        Raises:
            httpx.HTTPError: If the request fails
            FileNotFoundError: If the file doesn't exist
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        if path.stat().st_size > 10 * 1024 * 1024:
            return await self.upload_large_file_async(path)

        async with aiofiles.open(path, "rb") as f:
            files = {"file": (path.name, await f.read())}
            response = await self._async_client.post(
                url="files",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                },
                files=files,
            )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as e:
                print(e.response.text)
                raise e
            resp = response.json()
            return resp.get("id")

    def upload_large_file(self, path: Union[str, Path]) -> str:
        """
        Upload a large file to the Tensorlake. A large file is a file larger than 10 MB.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        sha256_checksum = self.__calculate_checksum_sha256__(path)
        file_size = path.stat().st_size
        # Initialize upload request
        init_response = self._client.post(
            url="files_large",
            headers=self.__headers__(),
            json={
                "sha256_checksum": sha256_checksum,
                "file_size": file_size,
                "filename": path.name,
            },
        )
        init_response.raise_for_status()
        init_response_json = init_response.json()

        presign_id = init_response_json.get("id")
        if presign_id.startswith("tensorlake-"):
            return presign_id

        progress_bar = tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            desc=path.name,
        )

        with open(path, "rb") as f:
            with httpx.Client() as upload_client:

                def file_chunk_generator():
                    while chunk := f.read(1024 * 1024):
                        progress_bar.update(len(chunk))
                        yield chunk

                upload_response = upload_client.put(
                    url=init_response_json.get("presigned_url"),
                    data=file_chunk_generator(),
                    headers={
                        "Content-Type": self.__get_mime_type__(path),
                        "Content-Length": str(file_size),
                        "x-amz-checksum-sha256": base64.b64encode(
                            bytes.fromhex(sha256_checksum)
                        ).decode(),
                        "x-amz-sdk-checksum-algorithm": "SHA256",
                    },
                    timeout=httpx.Timeout(None),
                )
                upload_response.raise_for_status()

        progress_bar.close()
        print(f"{path.name} upload complete!")

        # Finalize the upload
        finalize_response = self._client.post(
            url=f"files_large/{presign_id}", headers=self.__headers__()
        )
        finalize_response.raise_for_status()
        return finalize_response.json().get("id")

    def __calculate_checksum_sha256__(self, path: Union[str, Path]) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    def __get_mime_type__(self, path: Union[str, Path]) -> str:
        """
        Get the MIME type of a file. If `python-magic` (libmagic) is installed, use it.
        Otherwise, fall back to `mimetypes`.

        Args:
            path (Union[str, Path]): The file path to check.

        Returns:
            str: The MIME type of the file, or "application/octet-stream" if unknown.
        """
        if sys.platform.startswith("win") or not _HAS_MAGIC:
            return mimetypes.guess_type(str(path))[0] or "application/octet-stream"

        mime = magic.Magic(mime=True)
        return mime.from_file(str(path))

    async def upload_large_file_async(self, path: Union[str, Path]) -> str:
        """
        Asynchronously upload large files to Tensorlake
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        checksum_sha256 = await self.__calculate_checksum_sha256_async__(path)
        file_size = path.stat().st_size
        filename = path.name

        init_response = await self._async_client.post(
            url="files_large",
            headers=self.__headers__(),
            json={
                "sha256_checksum": checksum_sha256,
                "file_size": file_size,
                "filename": filename,
            },
        )
        init_response.raise_for_status()
        init_response_json = init_response.json()

        presign_id = init_response_json.get("id")
        if presign_id.startswith("tensorlake-"):
            return presign_id

        progress_bar = async_tqdm(
            total=file_size,
            unit="B",
            unit_scale=True,
            desc=filename,
            disable=not sys.stdout.isatty(),
            leave=False,
        )

        async with httpx.AsyncClient() as upload_client:

            async def file_chunk_generator():
                async with aiofiles.open(path, "rb") as file:
                    while chunk := await file.read(1024 * 1024):
                        progress_bar.update(len(chunk))
                        yield chunk

            upload_response = await upload_client.put(
                url=init_response_json.get("presigned_url"),
                data=file_chunk_generator(),
                headers={
                    "Content-Type": self.__get_mime_type__(path),
                    "Content-Length": str(file_size),
                    "x-amz-checksum-sha256": base64.b64encode(
                        bytes.fromhex(checksum_sha256)
                    ).decode(),
                    "x-amz-sdk-checksum-algorithm": "SHA256",
                },
                timeout=httpx.Timeout(None),
            )
            upload_response.raise_for_status()

        progress_bar.set_description("")
        progress_bar.clear()
        progress_bar.close()
        sys.stdout.flush()
        print(f"{filename} upload complete!", flush=True)

        finalize_response = await self._async_client.post(
            url=f"files_large/{presign_id}", headers=self.__headers__()
        )
        finalize_response.raise_for_status()
        finalize_response_json = finalize_response.json()

        return finalize_response_json.get("id")

    async def __calculate_checksum_sha256_async__(self, path: Union[str, Path]) -> str:
        hasher = hashlib.sha256()
        async with aiofiles.open(path, "rb") as file:
            while chunk := await file.read(4096):
                hasher.update(chunk)
        return hasher.hexdigest()

    def __headers__(self):
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
