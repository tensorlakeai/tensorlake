from pydantic import BaseModel

from .handler import Handler, Request, Response

PROGRESS_UPDATE_PATH: str = "/progress/update"
PROGRESS_UPDATE_VERB: str = "POST"


class FunctionProgressUpdateRequest(BaseModel):
    request_id: str
    allocation_id: str
    function_name: str
    current: float
    total: float
    message: str | None
    attributes: dict[str, str] | None


class FunctionProgressUpdateResponse(BaseModel):
    pass


class BaseProgressUpdateHandler(Handler):
    """Base handler for processing function progress update requests."""

    def handle(self, request: Request) -> Response:
        progress_update_request = FunctionProgressUpdateRequest.model_validate_json(
            request.body
        )
        progress_update_response: FunctionProgressUpdateResponse = self._handle(
            progress_update_request
        )
        return Response(
            status_code=200,
            headers={"Content-Type": "application/json"},
            body=progress_update_response.model_dump_json().encode("utf-8"),
        )

    def _handle(
        self, request: FunctionProgressUpdateRequest
    ) -> FunctionProgressUpdateResponse:
        raise NotImplementedError(
            "BaseProgressUpdateHandler subclasses must implement _handle method."
        )
