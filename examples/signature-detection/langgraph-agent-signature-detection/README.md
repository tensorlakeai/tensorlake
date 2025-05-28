# Signature Detection with a LangGraph Agent
This is an example of a comprehensive document signature analysis system that combines Tensorlake's signature detection capability with LangGraph's conversational agent framework. 
The system provides automated signature detection and intelligent querying capabilities for document analysis workflows.

## Key Features
- Automated Signature Detection: Processes PDF/DOCX documents using Tensorlake's signature detection API
- Conversational AI Interface: LangGraph-powered agent for natural language queries about signature analysis
- Persistent Data Storage: Saves analysis results for future reference and querying

## Flow
- Standalone Processing: Document upload → Tensorlake signature detection using Tensorlake's DocumentAI API → JSON storage
- Conversational Agent: LangGraph agent with tool access to saved analysis data

### Example
```bash
Document Signature Analysis System
=============================================
1. Process document for signature detection
2. Chat about analyzed documents
3. Exit

Select option (1-3): 1
Enter document file path: real-estate-purchase-all-signed.pdf
Document Signature Detection
==================================================

SUCCESS!
Structured analysis: 
{'success': True, 'file_name': 'real-estate-purchase-all-signed.pdf', 'total_signatures': 21, 'total_pages': 10, 'pages_with_signatures': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 'summary': 'Found 21 signatures across 10 pages in real-estate-purchase-all-signed.pdf', 'data_saved_to': 'signature_analysis_data/real-estate-purchase-all-signedpdf_signature_analysis.json'}

You can now use the conversation agent to ask questions about this document!

Select option (1-3): 2

Found 1 analyzed document(s)
Signature Analysis Conversation
==================================================
Ask me questions about your analyzed documents!
Type 'quit' to exit.

You: how many signatures were there?
Assistant: The document contains a total of 21 signatures.

You: what pages have signatures
Assistant: Signatures are present on all pages of the document, which are pages 1 through 10.

You: which page has the most signatures?
Assistant: Page 10 has the most signatures, with a total of 3 signatures.

You: are all of these signatures or are some initials?
Assistant: The document contains both signatures and initials. The signatures are primarily found on page 10, while the other pages (1 through 9) contain initials.

You: who signed on page 10?
Assistant: On page 10, the following individuals signed the document:

1. Buyer Signature: Nova Ellison
2. Seller Signature: Juno Vega
3. Agent Signature: Aster Polaris (from Polaris Group LLC)

You: quit
Goodbye! 👋
Select option (1-3): 3
Goodbye! 👋
```

## Using this example
### 1. Set Environment Variables
1. Get your [Tensorlake API Key](https://docs.tensorlake.ai/accounts-and-access/api-keys) and OpenAI API Key.
2. Rename `.env.example` to `.env`
3. Fill in your API Keys in the `.env` file:
    ```bash
    OPENAI_API_KEY=your_openai_api_key
    TENSORLAKE_API_KEY=your_tensorlake_api_key
    ```

### 2. Python Dependencies
You need to install these dependencies individually, or by running:
```bash
pip install -r requirements.txt
```

**Full Dependency List:**
```bash
openai>=1.0.0
langchain>=0.1.0
langchain-openai>=0.1.0
langgraph>=0.1.0
tensorlake>=0.1.0
python-dotenv>=1.0.0
```

*Note:* We recommend you setup a virtual environment and you should have Python>3.8 installed