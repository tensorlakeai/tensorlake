class ApiException(Exception):
    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code

    def __str__(self):
        return f"{super().__str__()} (Status Code: {self.status_code})"


class GraphStillProcessing(Exception):
    def __init__(self) -> None:
        super().__init__("graph is still processing")
