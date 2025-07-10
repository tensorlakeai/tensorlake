# Interactive Signature Detection CLI

An interactive command-line interface for analyzing signatures in documents using TensorLake AI and LangGraph's conversational agent framework.

## Features

- **Interactive CLI**: Ask questions about documents in a conversational manner
- **Document Analysis**: Analyze PDF documents for signature detection
- **Persistent Memory**: The agent remembers context throughout the conversation
- **Document Caching**: Automatically caches parsed document data for faster subsequent queries
- **Flexible Input**: Support for both URLs and local file paths
- **Real-time Processing**: Get immediate responses to your questions
- **Cache Management**: View and clear cached documents

## Installation

1. Install required dependencies:
```bash
pip install langchain-tensorlake dotenv
```

2. Set up environment variables:
```bash
# Rename the .env.example to .env and set your environment variables
OPENAI_API_KEY=your_openai_api_key_here
TENSORLAKE_API_KEY=your_tensorlake_api_key_here
```

## Usage

### Basic Usage

Run the interactive CLI:
```bash
python interactive_signature_detection.py
```

### Workflow

1. **Specify Document**: When prompted, provide:
   - A URL to a document
   - An absolute local file path to a document  
   - Or press Enter to use the default sample document

2. **Ask Questions**: Once the document is loaded, you can ask questions like:
   - "How many signatures are in this document?"
   - "Who are the parties involved?"
   - "Are there any missing signatures?"
   - "What type of document is this?"
   - "What are the key terms in the document?"

3. **Available Commands**:
   - `help` or `?` - Show available commands and example questions
   - `new` or `change` - Switch to a different document
   - `cache list` - View all cached documents
   - `cache clear` - Clear all cached document data
   - `quit` or `exit` - Exit the application

### Example Session

```
$ python interactive_signature_detection.py

============================================================
Interactive Document Signature Analysis
============================================================
Welcome! Please specify the document you'd like to analyze.
You can provide:
- A URL to a PDF document
- An absolute local file path to a PDF document
- Press Enter to use the default sample document
------------------------------------------------------------
Document path or URL: [Press Enter for default]
Using default sample document: https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf

📄 Document loaded: https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf
🆕 This document will be analyzed on first question and cached for future use.
💬 You can now ask questions about this document!
   Type 'help' for available commands or 'quit' to exit.

❓ Your question: How many signatures are in this document?

🔍 Analyzing document and processing your question...
💾 Document analysis saved to cache for future use.

============================================================
🤖 Analysis Response:
============================================================
Based on my analysis of the document, I found 4 signatures total...
[Detailed response about signatures found]
------------------------------------------------------------

❓ Your question: Who are the parties involved?

🔍 Using cached document data for faster response...

============================================================
🤖 Analysis Response:
============================================================
Based on the previously analyzed content, the parties involved in this real estate transaction are...
[Detailed response about parties - using cached data]
------------------------------------------------------------

❓ Your question: cache list

📂 Cached Documents:
============================================================
• https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf
  Cached: 1704854400.123456
------------------------------------------------------------

❓ Your question: quit

👋 Thank you for using the Interactive Signature Detection System!
```

## Technical Details

- **Agent Framework**: Uses LangGraph's `create_react_agent` with memory persistence
- **Memory System**: Implements `MemorySaver` for conversation context
- **Document Processing**: Leverages `document_markdown_tool` from langchain-tensorlake
- **Caching System**: Local JSON-based caching with MD5 hash keys for document identification
- **Model**: Uses OpenAI's GPT-4o-mini for analysis
- **Threading**: Maintains conversation state with thread configuration
- **Storage**: Cache stored in `document_cache/` directory with index file

## Error Handling

The system includes comprehensive error handling for:
- Missing environment variables
- Invalid document paths
- Network connectivity issues
- API errors
- User interruption (Ctrl+C)

## Extensibility

The `InteractiveSignatureDetector` class can be easily extended to:
- Add new document types
- Implement additional analysis tools
- Customize the agent's behavior
- Add more sophisticated memory management
- Include additional file format support
- Implement cache expiration policies
- Add cache size limits and cleanup
- Support for distributed caching systems
