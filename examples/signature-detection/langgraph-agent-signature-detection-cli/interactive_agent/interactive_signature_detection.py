"""
Interactive Document Signature Analysis System

An interactive CLI system for detecting and analyzing signatures in documents using TensorLake AI 
through langchain-tensorlake and LangGraph's conversational agent framework.
Users can specify a document and then ask questions interactively.
"""

# Helper packages
import os
import sys
import json
import hashlib
from pathlib import Path
from dotenv import load_dotenv
import asyncio
from typing import Dict, Any, Optional

# LangGraph packages
from langchain_tensorlake import document_markdown_tool
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
TENSORLAKE_API_KEY = os.getenv("TENSORLAKE_API_KEY")

class InteractiveSignatureDetector:
    """Interactive signature detection system using LangGraph conversational agent with caching."""
    
    def __init__(self):
        """Initialize the interactive signature detector."""
        self.agent = None
        self.document_path = None
        self.thread_config = {"configurable": {"thread_id": "signature_analysis_session"}}
        self.memory = MemorySaver()
        self.cache_dir = Path("document_cache")
        self.document_cache = {}
        self._setup_cache_directory()
        self._setup_agent()
    
    def _setup_cache_directory(self):
        """Create cache directory if it doesn't exist."""
        self.cache_dir.mkdir(exist_ok=True)
        print(f"📂 Cache directory set up at: {self.cache_dir}")
        cache_index_file = self.cache_dir / "cache_index.json"
        
        # Load existing cache index
        if cache_index_file.exists():
            try:
                with open(cache_index_file, 'r') as f:
                    self.document_cache = json.load(f)
            except (json.JSONDecodeError, IOError):
                self.document_cache = {}
    
    def _get_document_hash(self, document_path: str) -> str:
        """Generate a hash for the document path to use as cache key."""
        return hashlib.md5(document_path.encode()).hexdigest()
    
    def _save_cache_index(self):
        """Save the cache index to disk."""
        cache_index_file = self.cache_dir / "cache_index.json"
        try:
            with open(cache_index_file, 'w') as f:
                json.dump(self.document_cache, f, indent=2)
            print(f"✅ Cache index saved: {cache_index_file}")
        except IOError as e:
            print(f"❌ Error: Could not save cache index: {e}")
        except Exception as e:
            print(f"❌ Unexpected error saving cache index: {e}")
    
    def _get_cached_content(self, document_path: str) -> Optional[str]:
        """Retrieve cached document content if available."""
        doc_hash = self._get_document_hash(document_path)
        
        if doc_hash in self.document_cache:
            cache_file = self.cache_dir / f"{doc_hash}.json"
            if cache_file.exists():
                try:
                    with open(cache_file, 'r') as f:
                        cache_data = json.load(f)
                        return cache_data.get('content')
                except (json.JSONDecodeError, IOError):
                    # Remove invalid cache entry
                    if doc_hash in self.document_cache:
                        del self.document_cache[doc_hash]
                        self._save_cache_index()
        
        return None
    
    def _save_cached_content(self, document_path: str, content: str):
        """Save document content to cache."""
        doc_hash = self._get_document_hash(document_path)
        cache_file = self.cache_dir / f"{doc_hash}.json"
        
        print(f"🔍 Debug: Attempting to save cache for document: {document_path}")
        print(f"🔍 Debug: Cache file path: {cache_file}")
        print(f"🔍 Debug: Document hash: {doc_hash}")
        
        cache_data = {
            'document_path': document_path,
            'content': content,
            'cached_at': asyncio.get_event_loop().time()
        }
        
        try:
            with open(cache_file, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            print(f"✅ Cache file written successfully: {cache_file}")
            
            # Update cache index
            self.document_cache[doc_hash] = {
                'document_path': document_path,
                'cache_file': f"{doc_hash}.json",
                'cached_at': cache_data['cached_at']
            }
            self._save_cache_index()
            print("✅ Cache index updated successfully!")
            
        except IOError as e:
            print(f"❌ Error: Could not save cache for document: {e}")
            print(f"🔍 Debug: Cache directory exists: {self.cache_dir.exists()}")
            print(f"🔍 Debug: Cache directory is writable: {os.access(self.cache_dir, os.W_OK)}")
        except Exception as e:
            print(f"❌ Unexpected error saving cache: {e}")
    
    def _clear_cache(self):
        """Clear all cached documents."""
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                cache_file.unlink()
            self.document_cache = {}
            print("🗑️ Cache cleared successfully!")
        except Exception as e:
            print(f"⚠️ Warning: Could not clear cache: {e}")
    
    def _list_cached_documents(self):
        """List all cached documents."""
        if not self.document_cache:
            print("📂 No documents in cache.")
            return
        
        print("\n📂 Cached Documents:")
        print("=" * 60)
        for doc_hash, info in self.document_cache.items():
            doc_path = info['document_path']
            cached_at = info['cached_at']
            print(f"• {doc_path}")
            print(f"  Cached: {cached_at}")
        print("-" * 60)
    
    def _setup_agent(self):
        """Set up the LangGraph conversational agent with memory."""
        prompt = """You are a helpful assistant specialized in document signature analysis.
        
        Your responsibilities:
        1. Help users analyze documents for signature detection
        2. Answer questions based on the document content and signature data
        3. Maintain context throughout the conversation
        4. Provide detailed insights about signatures, parties involved, and document analysis
        5. Use cached document data when available to provide faster responses
        
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
        - What are the key terms or clauses in the document?
        - When was the document created or executed?
        
        When a user asks about a document:
        1. First check if you have cached content for the document
        2. If cached content is available, use it to answer questions
        3. If no cached content is available, use the document_markdown_tool to analyze it
        
        Be conversational, helpful, and provide detailed analysis when requested.
        If you're using cached data, you can mention that you're using previously analyzed content for faster response.
        """
        
        self.agent = create_react_agent(
            model="openai:gpt-4o-mini",
            tools=[document_markdown_tool],
            prompt=prompt,
            checkpointer=self.memory,
            name="interactive-signature-agent"
        )
    
    def _get_document_path(self) -> str:
        """Get document path from user input."""
        print("\n" + "="*60)
        print("Interactive Document Signature Analysis")
        print("="*60)
        print("Welcome! Please specify the document you'd like to analyze.")
        print("You can provide:")
        print("- A URL to a PDF document")
        print("- An absolute local file path to a PDF document")
        print("- Press Enter to use the default sample document")
        print("-"*60)
        
        user_input = input("Document absolute path or URL: ").strip()
        
        if not user_input:
            # Use default sample document
            self.document_path = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf"
            print(f"Using default sample document: {self.document_path}")
        else:
            self.document_path = user_input
            print(f"Using document: {self.document_path}")

        return self.document_path

    def _print_help(self):
        """Print available commands and example questions."""
        print("\n" + "="*60)
        print("Available Commands:")
        print("="*60)
        print("- 'help' or '?' : Show this help message")
        print("- 'quit' or 'exit' : Exit the application")
        print("- 'new' or 'change' : Change to a different document")
        print("- 'cache list' : List all cached documents")
        print("- 'cache clear' : Clear document cache")
        print("- Any question about the document")
        print("\n" + "="*60)
        print("Example Questions:")
        print("="*60)
        print("- How many signatures are in this document?")
        print("- Who are the parties involved?")
        print("- Are there any missing signatures?")
        print("- What type of document is this?")
        print("- What are the key terms in the document?")
        print("- Which pages contain signatures?")
        print("- What is the date of the signatures?")
        print("-"*60)
    
    async def _process_question(self, question: str) -> str:
        """Process a user question using the agent with caching support."""
        try:
            # Check if we have cached content for this document
            cached_content = self._get_cached_content(self.document_path)
            
            if cached_content:
                print("\n🔍 Using cached document data for faster response...")
                # Create a question that includes the cached content
                contextual_question = f"""I have previously analyzed the document at {self.document_path}. 
                Here is the cached analysis content:
                
                {cached_content}
                
                Based on this cached analysis, please answer the following question: {question}
                
                Note: You can mention that you're using previously analyzed content for this faster response."""
            else:
                print("\n🔍 Analyzing document and processing your question...")
                # Include document context in the question for first-time analysis
                contextual_question = f"Please analyze the document at {self.document_path} and answer: {question}"
            
            # Invoke the agent with the question
            result = self.agent.invoke(
                {"messages": [{"role": "user", "content": contextual_question}]},
                config=self.thread_config
            )
            
            response_content = result["messages"][-1].content
            
            # If this was a fresh analysis (not from cache), save the result to cache
            if not cached_content:
                print("💾 Saving document analysis to cache for future use...")
                # Look for tool calls in the result messages to extract the raw document content
                tool_content = None
                for message in result.get("messages", []):
                    if hasattr(message, 'tool_calls') and message.tool_calls:
                        for tool_call in message.tool_calls:
                            if tool_call.get('name') == 'document_markdown_tool':
                                tool_content = "Tool used successfully"
                                break
                    # Also check for tool response messages
                    elif hasattr(message, 'content') and 'document_markdown_tool' in str(message):
                        tool_content = message.content
                        break
                
                # Save the analysis result to cache (using the full response)
                self._save_cached_content(self.document_path, response_content)
                print("✅ Document analysis saved to cache successfully!")
            
            return response_content
            
        except Exception as e:
            return f"❌ Error processing question: {str(e)}"
    
    async def run_interactive_session(self):
        """Run the main interactive session."""
        try:
            # Get document path
            self.document_path = self._get_document_path()
            
            # Show help
            self._print_help()
            
            print(f"\n📄 Document loaded: {self.document_path}")
            
            # Check if document is already cached
            if self._get_cached_content(self.document_path):
                print("💾 Found cached analysis for this document - responses will be faster!")
            else:
                print("🆕 This document will be analyzed on first question and cached for future use.")
            
            print("💬 You can now ask questions about this document!")
            print("   Type 'help' for available commands or 'quit' to exit.\n")
            
            while True:
                try:
                    # Get user input
                    user_input = input("\n❓ Your question: ").strip()
                    
                    if not user_input:
                        continue
                    
                    # Handle commands
                    if user_input.lower() in ['quit', 'exit', 'q']:
                        print("\n👋 Thank you for using the Interactive Signature Detection System!")
                        break
                    
                    elif user_input.lower() in ['help', '?']:
                        self._print_help()
                        continue
                    
                    elif user_input.lower() in ['new', 'change']:
                        self.document_path = self._get_document_path()
                        print(f"\n📄 Document changed to: {self.document_path}")
                        
                        # Check if new document is cached
                        if self._get_cached_content(self.document_path):
                            print("💾 Found cached analysis for this document - responses will be faster!")
                        else:
                            print("🆕 This document will be analyzed on first question and cached for future use.")
                        
                        print("💬 You can now ask questions about the new document!")
                        continue
                    
                    elif user_input.lower() == 'cache list':
                        self._list_cached_documents()
                        continue
                    
                    elif user_input.lower() == 'cache clear':
                        self._clear_cache()
                        continue
                    
                    # Process the question
                    response = await self._process_question(user_input)
                    
                    # Display response
                    print("\n" + "="*60)
                    print("🤖 Analysis Response:")
                    print("="*60)
                    print(response)
                    print("-"*60)
                    
                except KeyboardInterrupt:
                    print("\n\n👋 Session interrupted. Thank you for using the Interactive Signature Detection System!")
                    break
                except Exception as e:
                    print(f"\n❌ An error occurred: {str(e)}")
                    print("Please try again or type 'quit' to exit.")
        
        except KeyboardInterrupt:
            print("\n\n👋 Session interrupted. Thank you for using the Interactive Signature Detection System!")
        except Exception as e:
            print(f"\n❌ Failed to start interactive session: {str(e)}")

def main():
    """Main entry point for the interactive signature detection system."""
    # Check for required environment variables
    if not OPENAI_API_KEY:
        print("❌ Error: OPENAI_API_KEY environment variable is not set.")
        print("Please set your OpenAI API key in your environment or .env file.")
        sys.exit(1)
    
    if not TENSORLAKE_API_KEY:
        print("❌ Error: TENSORLAKE_API_KEY environment variable is not set.")
        print("Please set your TensorLake API key in your environment or .env file.")
        sys.exit(1)
    
    # Create and run the interactive detector
    detector = InteractiveSignatureDetector()
    
    try:
        asyncio.run(detector.run_interactive_session())
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
