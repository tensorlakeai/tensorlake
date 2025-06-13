import os

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from dotenv import load_dotenv

from src.tensorlake.integrations.agno.documents_parser import TensorLakeTools

load_dotenv()

TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")

# Basic agent with document parsing capabilities
agent = Agent(
    name="Document Parser Agent",
    instructions="I have a document that needs to be parsed. Please parse it and answer the question.",
    model=OpenAIChat(id="gpt-4o"),
    tools=[TensorLakeTools(
        api_key=TENSORLAKE_API_KEY,
        document_path="real-estate-purchase-all-signed.pdf"
    )],
    show_tool_calls=True
)

agent.print_response("How many signatures are found in this whole document")
