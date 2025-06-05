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

from helper_functions import extract_signature_data, save_analysis_data, SIGNATURE_DATA_DIR

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")

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