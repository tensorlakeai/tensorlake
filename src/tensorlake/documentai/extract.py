import os
from typing import Union, Type
from pydantic import BaseModel, Json

class StructuredExtractor:

    def __init__(self, api_key: str=""):
        self.api_key = api_key
        if not self.api_key:
            self.api_key = os.getenv("TENSORLAKE_API_KEY")

    def parse_document(self, path: str, schema: Union[Json, Type[BaseModel]], timeout: int) -> Json:
        """
        Parse a document.
        """
        return "hello"
    
    async def parse_document_async(self, path: str, schema: Union[Json, Type[BaseModel]]) -> Json:
        """
        Parse a document asynchronously.
        """
        return "hello"