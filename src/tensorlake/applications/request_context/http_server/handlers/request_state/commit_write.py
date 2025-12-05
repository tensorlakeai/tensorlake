from pydantic import BaseModel

from tensorlake.applications.blob_store import BLOB

from ..handler import Handler, Request, Response

COMMIT_WRITE_PATH: str = "/request_state/commit_write"
COMMIT_WRITE_VERB: str = "POST"


class CommitWriteRequest(BaseModel):
    request_id: str
    allocation_id: str
    state_key: str
    blob: BLOB


class CommitWriteResponse(BaseModel):
    pass


class BaseCommitWriteHandler(Handler):
    """Base handler for processing commit write requests."""

    def handle(self, request: Request) -> Response:
        request_payload = CommitWriteRequest.model_validate_json(request.body)
        response_payload: CommitWriteResponse = self._handle(request_payload)
        return Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=response_payload.model_dump_json().encode("utf-8"),
        )

    def _handle(self, request: CommitWriteRequest) -> CommitWriteResponse:
        raise NotImplementedError(
            "BaseCommitWriteHandler subclasses must implement _handle method."
        )
