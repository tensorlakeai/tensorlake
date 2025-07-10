"""
Document Signature Analysis System

A comprehensive system for detecting and analyzing signatures in documents using TensorLake AI through
langchain-tensorlake and LangGraph for conversational agent.
"""

# Helper packages
import os
from dotenv import load_dotenv
import asyncio

# LangGraph packages
from langchain_tensorlake import document_markdown_tool
from langgraph.prebuilt import create_react_agent

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")

# Function to analyze signatures in a document using LangGraph agent
# This function creates a LangGraph agent that uses the document_markdown_tool from langchain_tensorlake
# to process the document and answer questions about detected signatures.
# The agent is designed to handle questions related to signature detection, such as the number of signatures
# found, the parties involved, and any missing signatures.
async def analyze_signatures_agents(path: str, questions: str):
    """Parses the given document for signature data and analyzes it using a LangGraph agent
    powered by the langchain-tensorlake tool.

    Args:
        path (str): Path to the document (PDF).
        questions (str): Questions to ask about the detected signatures.

    Returns:
        str: Final agent response analyzing the signatures.
    """

    # Build prompt
    prompt = f"""You are a helpful assistant that answers questions about documents with signature detection data.
                Your responsibilities:
                1. Answer questions based on that loaded data
                2. Help users understand the signature analysis results

                You can answer questions like:
                - How many signatures were found?
                - Which pages contain signatures?
                - Who signed the document?
                - What does the content say around signatures?
                - What type of document is this?
                - Who are the parties involved?
                - What is the date of the signature?
                - Did each party sign the document?
                - Are there any missing signatures on any pages?
                - Which property is missing signatures?
                - Who is the agent for the properties missing signatures?

                Please analyze the above parsed output and answer the questions provided by the user.
                """

    agent = create_react_agent(
        model="openai:gpt-4o-mini",
        tools=[document_markdown_tool],
        prompt=(prompt),
        name="real-estate-agent"
    )

    print("Processing document with signature detection...")

    # Run agent on prompt
    result = agent.invoke({"messages": [{"role": "user", "content": questions}]})

    # Print the result
    print("Analysis results:\n", result["messages"][-1].content)

# Run an example analysis on a real estate document for signature insights
if __name__ == "__main__":

    # This is using a sample real estate document with signatures
    # You can replace this with any PDF document containing signatures
    path = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf"

    questions = f"1. How many signatures were detected in the document found at {path} and who are the parties involved?\n \
        2. What contextual information can you extract about any signatures in the document found at {path}?\n \
        3. Are there any missing signatures on any pages in the document found at {path}?"

    # run the example
    asyncio.run(analyze_signatures_agents(path, questions))
