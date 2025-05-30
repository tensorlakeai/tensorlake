"""
Document Signature Analysis System

A comprehensive system for detecting and analyzing signatures in documents using TensorLake AI
and LangGraph for conversational analysis. The system provides:

1. Standalone signature detection and processing
2. Conversational AI agent for querying signature analysis results
3. Persistent storage of analysis data for future reference
"""

import os
import time
import json
import logging
from typing import Dict, Any, Optional, TypedDict, Annotated, List
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

from helper_functions import extract_signature_data, save_analysis_data

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")
SIGNATURE_DATA_DIR = "signature_analysis_data"      # signature data analysis will be stored here

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global state for tracking processed files
_last_processed_file_name: Optional[str] = None


def detect_signatures_in_document(file_path: str) -> Dict[str, Any]:
    """
    Complete signature detection pipeline - uploads document, processes it, and saves results.
    Args:
        file_path: Path to the document file to analyze
    Returns:
        Dictionary containing signature analysis results
    """
    global _last_processed_file_name
    # Ensure data directory to save signature analysis data exists
    Path(SIGNATURE_DATA_DIR).mkdir(exist_ok=True)

    if not Path(file_path).exists():
        return {"success": False, "error": f"File not found: {file_path}"}

    file_name = Path(file_path).name
    _last_processed_file_name = file_name

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

    global _last_processed_file_name
    file_to_load = _last_processed_file_name

    if not file_to_load:
        # Find most recent analysis file
        try:
            analysis_files = list(Path(SIGNATURE_DATA_DIR).glob("*_signature_analysis.json"))
            if not analysis_files:
                return {"error": "No signature analysis files found in the directory."}

            # Get the most recent file by modification time
            latest_file = max(analysis_files, key=lambda x: x.stat().st_mtime)

            with open(latest_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            return {
                "success": True,
                "data": data,
                "file_name": data.get("file_name", "Unknown"),
                "note": f"Loaded most recent analysis: {latest_file.name}"
            }

        except Exception as e:
            logger.error(f"Failed to find analysis files: {str(e)}")
            return {"error": f"Failed to find analysis files: {str(e)}"}

    # Load specific file
    json_path = Path(SIGNATURE_DATA_DIR) / file_to_load

    if not json_path.exists():
        return {"error": f"No signature analysis data found for {file_to_load}"}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return {
            "success": True,
            "data": data,
            "file_name": file_to_load
        }
    except Exception as e:
        logger.error(f"Failed to load data for {file_to_load}: {str(e)}")
        return {"error": f"Failed to load data: {str(e)}"}


class ConversationState(TypedDict):
    """State schema for the signature conversation agent"""
    messages: Annotated[List, add_messages]


class SignatureConversationAgent:
    """
    LangGraph-based conversational agent for signature analysis queries.
    This agent can answer questions about previously analyzed documents, providing insights about signatures, and parties involved.
    """

    SYSTEM_PROMPT = """You are a helpful assistant that answers questions about PREVIOUSLY ANALYZED documents with signature detection data.

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

    def chat(self, conversation_history: Optional[List] = None) -> None:
        """
        Start an interactive chat session.
        """
        print("Signature Analysis Conversation")
        print("=" * 50)
        print("Ask me questions about your analyzed documents!")
        print("Type 'quit' to exit.\n")

        state = {"messages": conversation_history or []}

        while True:
            try:
                user_input = input("You: ").strip()

                if user_input.lower() in ['quit', 'exit', 'q']:
                    print("Goodbye! 👋")
                    break

                if not user_input:
                    continue

                # Add user message and process
                state["messages"].append(HumanMessage(content=user_input))
                final_state = self.graph.invoke(state)

                # Display response
                for message in reversed(final_state["messages"]):
                    if isinstance(message, AIMessage):
                        print(f"Assistant: {message.content}\n")
                        break

                # Update state
                state = final_state

            except KeyboardInterrupt:
                print("\nGoodbye! 👋")
                break
            except Exception as e:
                logger.error(f"Error in conversation: {str(e)}")
                print(f"Error: {e}\n")


def main() -> None:
    """Main CLI interface for the signature analysis system"""
    print("Document Signature Analysis System")
    print("=" * 45)
    print("1. Process document for signature detection")
    print("2. Chat about analyzed documents")
    print("3. Exit")
    print()

    while True:
        try:
            choice = input("Select option (1-3): ").strip()

            if choice == "1":
                file_path = input("Enter document file path: ").strip()
                if file_path:
                    print("Processing document...")
                    result = detect_signatures_in_document(file_path)

                    if result.get("success"):
                        print("\nSUCCESS!")
                        print(f"Analysis: {result['summary']}")
                        print(f"Data saved to: {result['data_saved_to']}")
                        print("\nYou can now use option 2 to ask questions about this document!")
                    else:
                        print(f"\nFAILED: {result.get('error', 'Unknown error')}")
                print()

            elif choice == "2":
                # Check for existing analysis files
                Path(SIGNATURE_DATA_DIR).mkdir(exist_ok=True)
                analysis_files = list(Path(SIGNATURE_DATA_DIR).glob("*_signature_analysis.json"))

                if not analysis_files:
                    print("No signature analysis files found!")
                    print("Please process some documents first using option 1.")
                else:
                    print(f"Found {len(analysis_files)} analyzed document(s)")
                    agent = SignatureConversationAgent()
                    agent.chat()

            elif choice == "3":
                print("Goodbye! 👋")
                break
            else:
                print("Invalid choice. Please select 1, 2, or 3.")

        except KeyboardInterrupt:
            print("\nGoodbye! 👋")
            break
        except Exception as e:
            logger.error(f"Error in main menu: {str(e)}")
            print(f"Error: {e}")


if __name__ == "__main__":
    # Run and test the flow
    main()
