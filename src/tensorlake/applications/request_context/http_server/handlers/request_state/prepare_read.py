from pydantic import BaseModel

from tensorlake.applications.blob_store import BLOB

from ..handler import Handler, Request, Response

PREPARE_READ_PATH: str = "/request_state/prepare_read"
PREPARE_READ_VERB: str = "POST"


class PrepareReadRequest(BaseModel):
    request_id: str
    allocation_id: str
    state_key: str


class PrepareReadResponse(BaseModel):
    # None if the state does not exist.
    blob: BLOB | None


class BasePrepareReadHandler(Handler):
    """Base handler for processing prepare read requests."""

    def handle(self, request: Request) -> Response:
        request_payload = PrepareReadRequest.model_validate_json(request.body)
        response_payload: PrepareReadResponse = self._handle(request_payload)
        return Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=response_payload.model_dump_json().encode("utf-8"),
        )

    def _handle(self, request: PrepareReadRequest) -> PrepareReadResponse:
        raise NotImplementedError(
            "BasePrepareReadHandler subclasses must implement _handle method."
        )
