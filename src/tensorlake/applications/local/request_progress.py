from ..interface.request_context import FunctionProgress


class LocalFunctionProgress(FunctionProgress):
    """FunctionProgress that tracks the progress of a local function call."""

    def __init__(self):
        super().__init__()

    def update(self, current: float, total: float) -> None:
        print(f"Progress update, current: {current}, total: {total}")
