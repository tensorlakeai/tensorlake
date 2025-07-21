# Signature Detection with a LangGraph Agent
This is an example of a comprehensive document signature analysis system that combines Tensorlake's Contextual Signature Detection with LangGraph's conversational agent framework. 

The system provides automated signature detection and intelligent querying capabilities for document analysis workflows.

## Key Features
- Automated Signature Detection: Processes documents containing signatures using [Tensorlake's Contextual Signature Detection](https://docs.tensorlake.ai/document-ingestion/parsing/signature)

## Using this example
### 0. Prerequisites
- Python 3.10+
- An [OpenAI API key](https://platform.openai.com/api-keys)
- A [Tensorlake API key](https://docs.tensorlake.ai/accounts-and-access/api-keys)
- Some [sample real estate documents](https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf)
  - You can also find a few other documents to test with in the `/documents` folder 
- [Optional] A [virtual Python environment](https://docs.python.org/3/library/venv.html) to keep dependencies isolated

### 1. Set Environment Variables
1. Get your [Tensorlake API key](https://docs.tensorlake.ai/accounts-and-access/api-keys) and [OpenAI API key](https://platform.openai.com/api-keys).
2. Rename `.env.example` to `.env`
3. Fill in your API Keys in the `.env` file:
    ```bash
    OPENAI_API_KEY=your_openai_api_key
    TENSORLAKE_API_KEY=your_tensorlake_api_key
    ```

### 2. [Optional] Set up a virtual environment:
On Mac:
```bash
python -m venv venv
source venv/bin/activate
```

On Windows:
```bash
python -m venv venv
venv\Scripts\activate
```

And when you're done:
```bash
deactivate venv
```
works on both Mac and Windows

### 3. Python Dependencies
You only need to install the `langchain-tensorlake` package, which will install the other
needed LangChain dependencies. 

**Note: For this example we are loading environment variables using dotenv, so that also needs to be installed.**
```bash
pip install langchain-tensorlake dotenv
```

*Note:* We recommend you setup a virtual environment and you should have Python>3.10 installed

### 4. Run the example in the CLI
Running the example is fairly straightforward, simply run:
```bash
python signature_detection_langgraph_agent.py
```