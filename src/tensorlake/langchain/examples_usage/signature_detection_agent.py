import asyncio
import os
from typing import Annotated, TypedDict, Union, List
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from langchain_openai import ChatOpenAI
from tensorlake.documentai.parse import ChunkingStrategy, TableOutputMode

from ..documents import DocumentParserOptions, document_markdown_tool

# Set API keys
os.environ["TENSORLAKE_API_KEY"] = "your_tensorlake_api_key_here"
os.environ["OPENAI_API_KEY"] = "your_openai_api_key_here"

# Define the agent state
class AgentState(TypedDict):
    messages: Annotated[list, add_messages]


# Agent node - decides whether to use tools
async def agent_node(state: AgentState):
    model = ChatOpenAI(
        model="gpt-4o",
        temperature=0.1
    ).bind_tools([document_markdown_tool])

    response = await model.ainvoke(state["messages"])
    return {"messages": [response]}


# Conditional Logic for Tool Use
def should_continue(state: AgentState):
    last_message = state["messages"][-1]
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tools"
    return END


# LangGraph Workflow Setup
workflow = StateGraph(AgentState)
workflow.add_node("agent", agent_node)
workflow.add_node("tools", ToolNode([document_markdown_tool]))
workflow.set_entry_point("agent")
workflow.add_conditional_edges("agent", should_continue, {"tools": "tools", END: END})
workflow.add_edge("tools", "agent")
app = workflow.compile()


# Prompt builder
def build_document_analysis_prompt(parsed_result: str, questions: Union[str, List[str]]) -> str:
    # Normalize single question to list
    if isinstance(questions, str):
        questions = [questions]

    question_block = "\n".join(f"{i + 1}. {q}" for i, q in enumerate(questions))

    return f"""You are an expert document analyzer.
I've processed a document and got this result:

{parsed_result}

Please analyze this output and answer the following:
{question_block}
"""


# # Document + Agent Pipeline
async def analyze_signatures_agents(
        file_path: str,
        parsing_options: DocumentParserOptions,
        questions: List[str]
) -> str:
    """Invoke the tool with parsing options, then use agent for analysis."""

    print("🔍 Processing document with signature detection...")

    # Pass parsing options and run the tool
    parsed_output = await document_markdown_tool.ainvoke({
        "path": file_path,
        "options": parsing_options
    })

    # Build prompt
    prompt = build_document_analysis_prompt(parsed_output, questions)

    # Run agent on prompt
    final_state = await app.ainvoke({
        "messages": [HumanMessage(content=prompt)]
    })

    return final_state["messages"][-1].content


async def example_without_structured_schema():

    # change to your own document path
    document_path = "path/to/your/document.pdf"
    # set the parsing options
    parsing_options = DocumentParserOptions(
        detect_signature=True,
        chunking_strategy=ChunkingStrategy.PAGE,
        table_output_mode=TableOutputMode.MARKDOWN,
        skip_ocr=True,
        timeout_seconds=300
    )

    analysis_questions = [
        "Were signatures detected in the document?",
        "What contextual information can you extract about any signatures?",
        "Are there any compliance or workflow implications based on signature presence or absence?"
    ]

    result = await analyze_signatures_agents(
        file_path=document_path,
        parsing_options=parsing_options,
        questions=analysis_questions
    )

    print("Analysis Result:\n\n", result)


async def example_with_stuctured_schema():

    # change to your own document path
    document_path = "path/to/your/document.pdf"
    # the structured schema that you want to set
    schema_json = """{
            "properties": {
                "buyer": {
                    "properties": {
                        "buyer_name": {"type": "string"},
                        "buyer_signature_date": {"type": "string"},
                        "buyer_signed": {"type": "boolean"}
                    },
                    "type": "object"
                },
                "seller": {
                    "properties": {
                        "seller_name": {"type": "string"},
                        "seller_signature_date": {"type": "string"},
                        "seller_signed": {"type": "boolean"}
                    },
                    "type": "object"
                }
            },
            "title": "real_estate_purchase_agreement",
            "type": "object"
        }"""
    # set the parsing options
    parsing_options = DocumentParserOptions(
        detect_signature=True,
        page_range="10",  # set the page range here, I am setting it to page 10 here
        extraction_schema=schema_json,
        skip_ocr=True,
        chunking_strategy=ChunkingStrategy.PAGE,
        table_output_mode=TableOutputMode.MARKDOWN,
        timeout_seconds=300
    )

    analysis_questions = "Give me the full parsed result"

    result = await analyze_signatures_agents(
        file_path=document_path,
        parsing_options=parsing_options,
        questions=analysis_questions
    )

    print("Analysis Result with strctured schema:\n\n", result)


if __name__ == "__main__":
    # example 1
    asyncio.run(example_without_structured_schema())
    # example 2
    asyncio.run(example_with_stuctured_schema())


