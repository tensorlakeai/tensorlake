from ..interface.request_context import RequestProgress


class LocalRequestProgress(RequestProgress):
    """RequestProgress that tracks the progress of a local request."""

    def __init__(self):
        super().__init__()

    def update(self, current: float, total: float) -> None:
        print(f"Progress update, current: {current}, total: {total}")
