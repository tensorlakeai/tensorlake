import time
import unittest
from typing import Any, List

from pydantic import BaseModel

from tensorlake.applications import (
    Future,
    Request,
    application,
    cls,
    function,
)
from tensorlake.applications import map as tl_map
from tensorlake.applications import (
    run_local_application,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

# This test doesn't verify much but it's used to simulate primary use case of the SDK
# and see how easy it is to express it using the current SDK UX.


class FakePDFChunk(BaseModel):
    content: str


class FakePDFParseResult(BaseModel):
    chunks: List[FakePDFChunk]


def fake_parse_pdf_service_call(file: str, page_range: str) -> FakePDFParseResult:
    time.sleep(0.001)  # Simulate network call
    start, end = map(int, page_range.split("-"))
    return FakePDFParseResult(
        chunks=[
            FakePDFChunk(content=f"Parsed page {i} content")
            for i in range(start, end + 1)
        ]
    )


class ChunkEmbedding(BaseModel):
    chunk: str
    embedding: float


class ChunkEmbeddings(BaseModel):
    chunk_embeddings: List[ChunkEmbedding]


class RequestPayload(BaseModel):
    url: str
    page_range: str


class ResponsePayload(BaseModel):
    chunks: List[ChunkEmbeddings]


@application()
@function(description="Fake PDF parse workflow")
def parse_pdf_api(payload: RequestPayload) -> ResponsePayload:
    # This is a blocking call of Tensorlake Function.
    # All function calls are blocking by default. To make a non-blocking call
    # users have to do .aio.call() on the Function/anything else they want to call
    # in a non-blocking way.
    response: FakePDFParseResult = fake_parse_pdf_service_call(
        file=payload.url,
        page_range=payload.page_range,
    )
    # Use map operation running in background as argument to other function calls.
    chunk_embeddings: Future = tl_map.aio.call(
        chunk_and_embed, [chunk.content for chunk in response.chunks]
    )

    # We can't return tl_map here because there's no parent function call node to which we can
    # attach map as a data dependency. Due to this limitation we have to use a blocking call.
    #
    # NB: when we support async Tensorlake Functions we will be able to
    # do `await chunk_embeddings` instead of calling result() here.
    chunks: List[ChunkEmbeddings] = chunk_embeddings.result()

    # Spawn a recurring background function to watch for the PDF file updates.
    watch_pdf_updates.aio.call_later(
        start_delay=60, url=payload.url, page_range=payload.page_range
    )
    return ResponsePayload(chunks=chunks)


@function()
def chunk_and_embed(page: str) -> ChunkEmbeddings:
    texts: List[str] = [page[i : i + 5] for i in range(0, len(page), 5)]
    embeddings: List[float] = [
        float(sum(byte for byte in text.encode("utf-8"))) for text in texts
    ]
    chunk_embeddings: List[ChunkEmbedding] = [
        ChunkEmbedding(chunk=text, embedding=embedding)
        for text, embedding in zip(texts, embeddings)
    ]
    output = ChunkEmbeddings(chunk_embeddings=chunk_embeddings)
    # Spawn IndexEmbedding function call in background to save the embeddings.
    # We're not interested in waiting for it to complete or value the function returned.
    IndexEmbedding().run.aio.call(output)

    return output


@cls()
class IndexEmbedding:
    def __init__(self):
        import os

        self.fake_embedding_db_uri = os.getenv(
            "MONGO_URI", "https://fake-embedding-index.com/api"
        )

    @function()
    def run(self, chunk_embeddings: ChunkEmbeddings) -> None:
        print(f"DB uri: {self.fake_embedding_db_uri}")
        print(chunk_embeddings)
        for chunk_embedding in chunk_embeddings.chunk_embeddings:
            print(
                "indexing embedding:",
                {
                    "text": chunk_embedding.chunk,
                    "embeddings": chunk_embedding.embedding,
                },
            )


@function()
def watch_pdf_updates(url: str, page_range: str) -> None:
    # Simulate fetching of the PDF file and checking for updates.
    time.sleep(0.1)
    print(f"Checked {url} for updates, no updates found.")
    # Schedule next check in 60 seconds.
    watch_pdf_updates.aio.call_later(start_delay=60, url=url, page_range=page_range)


class TestPDFParseDataWorkflow(unittest.TestCase):
    def test_local_api_call(self):
        request: Request = run_local_application(
            parse_pdf_api,
            RequestPayload(url="http://example.com/sample.pdf", page_range="1-5"),
        )
        payload: ResponsePayload = request.output()
        self.assertEqual(len(payload.chunks), 5)

    def test_remote_api_call(self):
        deploy_applications(__file__)
        request: Request = run_remote_application(
            parse_pdf_api,
            RequestPayload(url="http://example.com/sample.pdf", page_range="1-5"),
        )
        payload: ResponsePayload = request.output()
        self.assertEqual(len(payload.chunks), 5)


if __name__ == "__main__":
    unittest.main()
