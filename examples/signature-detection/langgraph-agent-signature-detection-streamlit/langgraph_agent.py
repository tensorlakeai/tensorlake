import os
import time
import json
import logging
from typing import Dict, Any, TypedDict, Annotated, List
from pathlib import Path
from dotenv import load_dotenv

# LangGraph and LangChain imports
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolNode
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.tools import tool
from langchain_openai import ChatOpenAI

# TensorLake imports
from tensorlake.documentai import DocumentAI, ParsingOptions, ExtractionOptions
from tensorlake.documentai.parse import ChunkingStrategy, TableParsingStrategy, TableOutputMode

from helper_functions import extract_signature_data, save_analysis_data, SIGNATURE_DATA_DIR

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def detect_signatures_in_document(file_path: str) -> Dict[str, Any]:
    """
    Complete signature detection pipeline - uploads document, processes it, and saves results.
    Args:
        file_path: Path to the document file to analyze
    Returns:
        Dictionary containing signature analysis results
    """
    # Ensure data directory to save signature analysis data exists
    Path(SIGNATURE_DATA_DIR).mkdir(exist_ok=True)

    if not Path(file_path).exists():
        return {"success": False, "error": f"File not found: {file_path}"}

    file_name = Path(file_path).name

    try:
        doc_ai = DocumentAI(api_key=TENSORLAKE_API_KEY)

        # Upload a document to TensorLake for processing
        file_id = doc_ai.upload(path=file_path)

        # Configure parsing options for signature detection
        options = ParsingOptions(
            chunk_strategy=ChunkingStrategy.NONE,
            table_parsing_strategy=TableParsingStrategy.TSR,
            table_output_mode=TableOutputMode.HTML,
            detect_signature=True,
            extraction_options=ExtractionOptions(skip_ocr=True),
        )

        # Start parsing job
        job_id = doc_ai.parse(file_id, options=options)
        logger.info(f"Parsing job started with ID: {job_id}")

        # Poll for completion with timeout
        start_time = time.time()
        max_wait_time = 300  # 5 minutes max
        while time.time() - start_time < max_wait_time:
            result = doc_ai.get_job(job_id)  # Signature detection result after parsing the document
            elapsed = time.time() - start_time

            if result.status in ["pending", "processing"]:
                logger.info(f"Job status: {result.status} ({elapsed:.0f}s elapsed)")
                time.sleep(5)
            elif result.status == "successful":
                logger.info(f"Processing completed successfully in {elapsed:.0f} seconds")
                break
            else:
                return {"success": False, "error": f"Job failed with status: {result.status}"}
        else:
            return {"success": False, "error": f"Processing timeout after {max_wait_time} seconds"}

        # Extract signature data from results
        signature_data = extract_signature_data(result, file_name, file_path)
        # Save analysis to JSON file
        json_path = save_analysis_data(signature_data, file_name)

        # Return summary
        return {
            "success": True,
            "file_name": file_name,
            "total_signatures": signature_data["total_signatures"],
            "total_pages": signature_data["total_pages"],
            "pages_with_signatures": signature_data["pages_with_signatures"],
            "signature_data": signature_data["signatures_per_page"],
            "summary": f"Found {signature_data['total_signatures']} signatures across {len(signature_data['pages_with_signatures'])} pages in {file_name}",
            "data_saved_to": json_path
        }

    except Exception as e:
        logger.error(f"Processing failed for {file_path}: {str(e)}")
        return {
            "success": False,
            "error": f"Processing failed: {str(e)}"
        }


@tool
def load_signature_analysis_data() -> Dict[str, Any]:
    """Load saved signature analysis data for conversational queries."""
    Path(SIGNATURE_DATA_DIR).mkdir(exist_ok=True)

    try:
        analysis_files = list(Path(SIGNATURE_DATA_DIR).glob("*_signature_analysis.json"))
        if not analysis_files:
            return {"error": "No signature analysis files found in the directory."}

        all_data = []

        for file_path in analysis_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    all_data.append(data)

            except Exception as e:
                logger.error(f"Failed to load {file_path}: {str(e)}")
                continue

        if not all_data:
            return {"error": "No valid signature analysis files could be loaded."}

        # Calculate total document files
        total_files = len(all_data)

        return {
            "success": True,
            "total_files_analyzed": total_files,
            "detailed_data": all_data,
            "note": f"Loaded analysis data from {total_files} document(s)"
        }

    except Exception as e:
        logger.error(f"Failed to load analysis files from the directory: {str(e)}")
        return {"error": f"Failed to load analysis files from the directory: {str(e)}"}


class ConversationState(TypedDict):
    """State schema for the signature conversation agent"""
    messages: Annotated[List, add_messages]


class SignatureConversationAgent:
    """
    LangGraph-based conversational agent for signature analysis queries.
    This agent can answer questions about previously analyzed documents, providing insights about signatures, and parties involved.
    """

    SYSTEM_PROMPT = """You are a helpful assistant that answers questions about PREVIOUSLY ANALYZED documents with contextual signature detection data.

IMPORTANT: You can ONLY answer questions about documents that have ALREADY been processed and saved. You do NOT process new documents - that's done separately.

Your responsibilities:
1. Use the load_signature_analysis_data tool to load saved analysis data
2. Answer questions based on that loaded data
3. Help users understand the signature analysis results

ALWAYS start by calling the load_signature_analysis_data tool first, even if no specific file is mentioned. This tool will automatically load the most recent analysis if no file name is provided.

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

WORKFLOW:
1. FIRST: Call load_signature_analysis_data tool (with or without file_name)
2. THEN: Answer the user's question based on the loaded data

Do NOT ask users for file paths - you work with already processed data only.
If the tool returns an error (no data found), explain that no analysis data is available and the user needs to process documents first."""

    def __init__(self, model: str = "gpt-4o", temperature: float = 0.1):
        """Initialize the signature conversation agent."""
        self.model = ChatOpenAI(model=model, temperature=temperature)
        self.tools = [load_signature_analysis_data]
        self.model_with_tools = self.model.bind_tools(self.tools)
        self.graph = self._create_graph()

    def _create_graph(self) -> StateGraph:
        """Create the LangGraph workflow for conversation management"""
        workflow = StateGraph(ConversationState)

        # Add nodes
        workflow.add_node("agent", self._agent_node)
        workflow.add_node("tools", ToolNode(self.tools))

        # Set entry point
        workflow.set_entry_point("agent")

        # Add edges
        workflow.add_conditional_edges(
            "agent",
            self._should_continue,
            {
                "continue": "tools",
                "end": END
            }
        )
        workflow.add_edge("tools", "agent")
        return workflow.compile()

    def _agent_node(self, state: ConversationState) -> ConversationState:
        """Main agent reasoning node"""
        messages = state["messages"]
        # Add system message if not present
        if not any(isinstance(msg, SystemMessage) for msg in messages):
            messages = [SystemMessage(content=self.SYSTEM_PROMPT)] + messages
        response = self.model_with_tools.invoke(messages)
        return {"messages": [response]}

    def _should_continue(self, state: ConversationState) -> str:
        """Determine whether to continue to tools or end the conversation"""
        last_message = state["messages"][-1]
        if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
            return "continue"
        return "end"

    def invoke(self, message: str) -> str:
        """
        Process a single message and return the response.
        """
        initial_state = {"messages": [HumanMessage(content=message)]}
        final_state = self.graph.invoke(initial_state)

        # Extract the last AI message
        for message in reversed(final_state["messages"]):
            if isinstance(message, AIMessage):
                return message.content

        return "No response generated."
