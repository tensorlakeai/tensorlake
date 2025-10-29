from .api_client import APIClient


class APIClientContextManager:
    """A context manager that uses provided APIClient or creates a new one from environment.

    If a new APIClient is created, it will be closed when exiting the context.
    """

    def __init__(
        self,
        api_client: APIClient | None = None,
    ):
        self._api_client: APIClient = (
            api_client if api_client is not None else APIClient()
        )
        self._should_close_client: bool = api_client is None

    def __enter__(self) -> APIClient:
        return self._api_client

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._should_close_client:
            self._api_client.close()
