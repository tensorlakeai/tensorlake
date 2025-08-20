class File:
    def __init__(self, content: bytes, content_type: str):
        self._content: bytes = content
        self._content_type: str = content_type

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def content_type(self) -> str:
        return self._content_type
