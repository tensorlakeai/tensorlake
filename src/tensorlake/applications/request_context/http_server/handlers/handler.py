from dataclasses import dataclass


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


@dataclass
class Response:
    status_code: int
    headers: dict[str, str]
    body: bytes


class Handler:
    def handle(self, request: Request) -> Response:
        """Handles an incoming HTTP request and returns a response.

        Any exception raised by the handler will result in a 500 Internal Server Error
        being returned to the client with the exception message in the response body.
        """
        raise NotImplementedError("Handler subclasses must implement handle method.")
