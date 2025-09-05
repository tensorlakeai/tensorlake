import time
import unittest
from typing import Any, List

from pydantic import BaseModel

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake


class FakePDFChunk(BaseModel):
    content: str


class FakePDFParseResult(BaseModel):
    chunks: List[FakePDFChunk]


def fake_parse_pdf_service_call(file: str, page_range: str) -> FakePDFParseResult:
    time.sleep(0.1)  # Simulate network call
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


@tensorlake.api()
@tensorlake.function(description="Fake PDF parse workflow")
def parse_pdf_api(
    ctx: tensorlake.RequestContext, payload: RequestPayload
) -> ResponsePayload:
    # Right now it's hard to understand which function call is a regular function call and which one is remote
    # Tensorlake call.
    response: FakePDFParseResult = fake_parse_pdf_service_call(
        file=payload.url,
        page_range=payload.page_range,
    )
    # To tell SDK to deconstruct the futures list and track the FunctionCalls inside of it we have
    # to wrap it into something we can recognize when building AST, i.e. tensorlake.map.
    chunk_embeddings = tensorlake.map(
        chunk_and_embed, [chunk.content for chunk in response.chunks]
    )
    # We can't return tensorlake.map here because there's no parent function call node to which we can
    # attach map as a data dependency. Due to this limitation we have to use a separate response creation
    # function which gets called once all the futures in map are resolved.
    return chunk_embeddings_to_response_payload(chunk_embeddings)


@tensorlake.function()
def chunk_embeddings_to_response_payload(
    chunks: List[ChunkEmbeddings],
) -> ResponsePayload:
    return ResponsePayload(chunks=chunks)


@tensorlake.function()
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
    # Spawning subgraphs without returning their outputs will be implemented later.
    # i.e. `tensorlake.spawn(IndexEmbedding().run(output))`
    # So we have to workaround this by returning `IndexEmbedding().run(output)` here.
    return first_argument(output, IndexEmbedding().run(output))


@tensorlake.function()
def first_argument(arg1: Any, arg2: Any) -> Any:
    return arg1


@tensorlake.cls()
class IndexEmbedding:
    def __init__(self):
        import os

        self.fake_embedding_db_uri = os.getenv(
            "MONGO_URI", "https://fake-embedding-index.com/api"
        )

    @tensorlake.function()
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


class TestPDFParseDataWorkflow(unittest.TestCase):
    def test_local_api_call(self):
        request = tensorlake.call_local_api(
            parse_pdf_api,
            RequestPayload(url="http://example.com/sample.pdf", page_range="1-5"),
        )
        print(request.output())


if __name__ == "__main__":
    unittest.main()
