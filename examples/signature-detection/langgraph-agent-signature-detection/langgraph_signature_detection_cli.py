"""
CLI Interface for Tensorlake's Contextual Signature Detection with LangGraph

This module provides a command-line interface for:
1. Processing documents for signature detection
2. Interactive chat with the signature detection agent
3. Managing analyzed document data

Usage:
    python langgraph_signature_detection_sli.py
"""

import logging
from pathlib import Path
from signature_detection_langgraph_agent import (
    detect_signatures_in_document,
    SignatureConversationAgent,
    SIGNATURE_DATA_DIR
)

# Configure logging for CLI
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


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
