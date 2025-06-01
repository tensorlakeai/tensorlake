from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field

class DocumentParserOptions(BaseModel):
    """Options for parsing a document."""
    detect_signature: bool = Field(default=True, description="Whether to detect the presence of a signature in the document.")

def document_to_markdown_converter(path: str, options: DocumentParserOptions) -> str:
    """Parse a document from a given path into markdown.
    
    Args:
        path: The path to the document to parse.

    Returns:
        The parsed document.
    """
    with open(path, "r") as f:
        return f.read()


document_to_markdown_converter_tool = StructuredTool.from_function(
    name="document_to_markdown_converter",
    description="Parse a document from a given path into markdown.",
    args_schema=DocumentParserOptions,
    func=document_to_markdown_converter,
)