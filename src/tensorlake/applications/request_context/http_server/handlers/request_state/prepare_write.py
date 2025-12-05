from pydantic import BaseModel

from tensorlake.applications.blob_store import BLOB

from ..handler import Handler, Request, Response

PREPARE_WRITE_PATH: str = "/request_state/prepare_write"
PREPARE_WRITE_VERB: str = "POST"


class PrepareWriteRequest(BaseModel):
    request_id: str
    allocation_id: str
    state_key: str
    size: int


class PrepareWriteResponse(BaseModel):
    blob: BLOB


class BasePrepareWriteHandler(Handler):
    """Base handler for processing prepare write requests."""

    def handle(self, request: Request) -> Response:
        request_payload = PrepareWriteRequest.model_validate_json(request.body)
        response_payload: PrepareWriteResponse = self._handle(request_payload)
        return Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=response_payload.model_dump_json().encode("utf-8"),
        )

    def _handle(self, request: PrepareWriteRequest) -> PrepareWriteResponse:
        raise NotImplementedError(
            "BasePrepareWriteHandler subclasses must implement _handle method."
        )
