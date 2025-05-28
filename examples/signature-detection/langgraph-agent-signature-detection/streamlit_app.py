"""
Streamlit UI for Document Signature Detection System
"""

import streamlit as st
import os
import json
from pathlib import Path
import tempfile

# Import your existing modules
from signature_detection_langgraph_agent import (
    detect_signatures_in_document,
    SignatureConversationAgent,
    SIGNATURE_DATA_DIR
)

# Import LangChain message types for state management
from langchain_core.messages import HumanMessage, AIMessage

# Page configuration
st.set_page_config(
    page_title="SignatureAI - Document Signature Detection",
    page_icon="✍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }

    .sidebar-info {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize Streamlit session state variables"""
    if 'chat_history' not in st.session_state:
        st.session_state.chat_history = []
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'processed_files' not in st.session_state:
        st.session_state.processed_files = []
    if 'agent' not in st.session_state:
        st.session_state.agent = None
    if 'langgraph_state' not in st.session_state:
        st.session_state.langgraph_state = {"messages": []}


def load_processed_files():
    """Load list of previously processed files"""
    try:
        Path(SIGNATURE_DATA_DIR).mkdir(exist_ok=True)
        analysis_files = list(Path(SIGNATURE_DATA_DIR).glob("*_signature_analysis.json"))

        files_info = []
        for file_path in analysis_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                files_info.append({
                    'filename': data.get('file_name', 'Unknown'),
                    'processed_date': data.get('processed_timestamp', 'Unknown'),
                    'total_signatures': data.get('total_signatures', 0),
                    'total_pages': data.get('total_pages', 0),
                    'json_path': str(file_path)
                })
            except Exception as e:
                continue

        return sorted(files_info, key=lambda x: x['processed_date'], reverse=True)
    except Exception as e:
        return []


def display_sidebar():
    """Display the sidebar with app information and processed files"""
    with st.sidebar:
        # App header
        st.markdown("""
        <div class="sidebar-info">
            <h2 style="margin: 0; color: white;">✍️ SignatureAI</h2>
            <p style="margin: 0.5rem 0 0 0; opacity: 0.9;">Intelligent Document Signature Detection by Tensorlake</p>
        </div>
        """, unsafe_allow_html=True)

        # App features
        st.markdown("### 🚀 Features")
        features = [
            "Upload documents and do automatic signature detection",
            "Detailed Analysis with Page-by-page signature mapping",
            "Ask questions about your signed data analysis",
            "Signature statistics and location data"
        ]

        for feature in features:
            st.markdown(f"- {feature}")

        st.markdown("---")

        # How it works
        st.markdown("### 📋 How It Works")
        st.markdown("""
        1. **Upload** your document
        2. **Process** with TensorLake AI
        3. **Review** signature analysis
        4. **Chat** to get insights
        """)

        st.markdown("---")

        # Previously processed files
        st.markdown("### 📚 Recent Documents")
        processed_files = load_processed_files()

        if processed_files:
            for i, file_info in enumerate(processed_files[:5]):  # Show last 5
                with st.expander(f"📄 {file_info['filename'][:20]}..."):
                    st.write(f"**Signatures Found:** {file_info['total_signatures']}")
                    st.write(f"**Total Pages:** {file_info['total_pages']}")
                    st.write(f"**Processed:** {file_info['processed_date'][:10]}")
        else:
            st.info("No documents processed yet")

        st.markdown("---")

        # Support info
        st.markdown("### 🔧 Support")
        st.markdown("""
        - **Supported formats:** PDF, DOCX, PNG, JPG
        - **Max file size:** 10MB
        - **Processing time:** 1-5 minutes
        """)


def process_document_tab():
    """Tab for document processing"""
    st.markdown("""
    <h2 class="main-header">📄 Document Upload & Processing</h2>
    <div style="text-align: center; color: #888; font-size: 0.9rem; margin-bottom: 1rem;">
        Built using Tensorlake with 🩵
    </div>
    """, unsafe_allow_html=True)

    # File upload section
    st.markdown("""
    <div> <h3>🔤 Upload Your Document</h3>
        <p>Upload any document containing signatures. Our AI will automatically detect and analyze all signatures found in your document.</p>
    </div>
    """, unsafe_allow_html=True)

    uploaded_file = st.file_uploader(
        "Choose a document file",
        type=['pdf', 'docx', 'doc', 'png', 'jpg', 'jpeg'],
        help="Supported formats: PDF, Word documents, and image files"
    )

    if uploaded_file is not None:
        # Display file info
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("📄 Filename", uploaded_file.name)
        with col2:
            st.metric("📏 File Size", f"{uploaded_file.size / 1024:.1f} KB")
        with col3:
            st.metric("🔤 File Type", uploaded_file.type)

        # Process button
        if st.button("🚀 Process Document", type="primary", use_container_width=True):
            with st.spinner("🔄 Processing document... This may take a few minutes."):
                # Save uploaded file temporarily
                with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{uploaded_file.name}") as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    temp_path = tmp_file.name

                try:
                    # Process the document
                    result = detect_signatures_in_document(temp_path)
                    if result.get("success"):
                        st.session_state.analysis_results = result

                        # Success message
                        st.markdown(f"""<h4>✅ Processing Completed Successfully!</h4>
                                        <p><strong>Summary:</strong> {result['summary']}</p>""", unsafe_allow_html=True)

                        # Display full result as JSON
                        st.markdown("### 📊 Analysis Results")
                        st.json(result)
                        st.markdown("""<strong>🎉 Success!</strong> You can now use the Chat tab to ask questions about this document.""", unsafe_allow_html=True)

                    else:
                        st.error(f"❌ Processing failed: {result.get('error', 'Unknown error')}")

                except Exception as e:
                    st.error(f"❌ An error occurred: {str(e)}")

                finally:
                    # Clean up temporary file
                    try:
                        os.unlink(temp_path)
                    except:
                        pass


def chat_tab():
    """Tab for conversational analysis"""
    st.markdown("""
        <h2 class="main-header">💬 Chat with Your Documents</h2>
        <div style="text-align: center; color: #888; font-size: 0.9rem; margin-bottom: 1rem;">
            Built using Tensorlake with 🩵
        </div>
        """, unsafe_allow_html=True)

    # Check if we have processed documents
    processed_files = load_processed_files()

    if not processed_files:
        st.markdown("""<h4>📄 No Documents Available</h4>
        <p>Please process a document first using the "Document Processing" tab before starting a conversation.</p>
        """, unsafe_allow_html=True)
        return

    # Initialize chat agent if not already done
    if st.session_state.agent is None:
        with st.spinner("🤖 Initializing Signature Conversation Agent..."):
            st.session_state.agent = SignatureConversationAgent()

    # Chat interface
    st.markdown("""
    <div> <h3>🤖 AI Assistant Ready</h3>
        <p>Ask me anything about your processed documents. I can help you understand signature analysis results, identify signers, and provide insights about your documents.</p>
    </div>
    """, unsafe_allow_html=True)

    # Example questions
    with st.expander("💡 Example Questions"):
        st.markdown("""
        - How many signatures were found in the document?
        - Which pages contain signatures?
        - What type of document is this?
        - Who are the parties involved?
        - What does the content say around the signatures?
        - Can you summarize the document content?
        """)

    # Display chat history first
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.write(message["content"])

    # Clear chat button
    if st.session_state.chat_history:
        if st.button("🗑️ Clear Chat History"):
            st.session_state.chat_history = []
            st.session_state.langgraph_state = {"messages": []}
            st.rerun()

    # Chat input
    if user_question := st.chat_input("Ask a question about your document..."):
        with st.chat_message("user"):
            st.write(user_question)

        # Add user message to both display history and LangGraph state
        st.session_state.chat_history.append({"role": "user", "content": user_question})
        st.session_state.langgraph_state["messages"].append(HumanMessage(content=user_question))

        # Get AI response
        with st.spinner("🤔 Thinking..."):
            try:
                final_state = st.session_state.agent.graph.invoke(st.session_state.langgraph_state)
                # Extract the last AI message
                response = None
                for message in reversed(final_state["messages"]):
                    if isinstance(message, AIMessage):
                        response = message.content
                        break

                if response:
                    st.write(response)
                    st.session_state.chat_history.append({"role": "assistant", "content": response})
                    st.session_state.langgraph_state = final_state
                else:
                    st.error("No response generated")
                    # Remove the user message if no response
                    if st.session_state.chat_history and st.session_state.chat_history[-1]["role"] == "user":
                        st.session_state.chat_history.pop()
                    if st.session_state.langgraph_state["messages"]:
                        st.session_state.langgraph_state["messages"].pop()

            except Exception as e:
                st.error(f"Error getting response: {str(e)}")
                # Remove the user message if there was an error
                if st.session_state.chat_history and st.session_state.chat_history[-1]["role"] == "user":
                    st.session_state.chat_history.pop()
                if st.session_state.langgraph_state["messages"]:
                    st.session_state.langgraph_state["messages"].pop()


def main():
    """Main Streamlit application"""
    initialize_session_state()
    display_sidebar()

    # Main content area with tabs
    tab1, tab2 = st.tabs(["📄 Document Processing", "💬 Chat Analysis"])

    with tab1:
        process_document_tab()

    with tab2:
        chat_tab()



if __name__ == "__main__":
    main()
