import json
from datetime import date
from typing import Dict, List, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import (
    ChunkingStrategy,
    ParsingOptions,
    StructuredExtractionOptions,
)

load_dotenv()


class Address(BaseModel):
    street: Optional[str] = Field(None, description="Street address")
    city: Optional[str] = Field(None, description="City")
    state: Optional[str] = Field(None, description="State/Province code or name")
    zip_code: Optional[str] = Field(None, description="Postal code")


class BankTransaction(BaseModel):
    transaction_deposit: Optional[float] = Field(None, description="Deposit amount")
    transaction_deposit_date: Optional[date] = Field(
        None, description="Date of the deposit"
    )
    transaction_deposit_description: Optional[str] = Field(
        None, description="Description of the deposit"
    )
    transaction_withdrawal: Optional[float] = Field(
        None, description="Withdrawal amount"
    )
    transaction_withdrawal_date: Optional[date] = Field(
        None, description="Date of the withdrawal"
    )
    transaction_withdrawal_description: Optional[str] = Field(
        None, description="Description of the withdrawal"
    )


class BankStatement(BaseModel):
    account_number: Optional[str] = Field(None, description="Bank account number")
    account_type: Optional[str] = Field(
        None, description="Type of the bank account (e.g. Checking/Savings)"
    )
    bank_address: Optional[Address] = Field(None, description="Address of the bank")
    bank_name: Optional[str] = Field(None, description="Name of the bank")
    client_address: Optional[Address] = Field(None, description="Address of the client")
    client_name: Optional[str] = Field(None, description="Name of the client")
    ending_balance: Optional[float] = Field(
        None, description="Ending balance for the period"
    )
    starting_balance: Optional[float] = Field(
        None, description="Starting balance for the period"
    )
    statement_date: Optional[date] = Field(
        None, description="Overall statement date if applicable"
    )
    statement_start_date: Optional[date] = Field(
        None, description="Start date of the bank statement"
    )
    statement_end_date: Optional[date] = Field(
        None, description="End date of the bank statement"
    )
    table_item: Optional[List[BankTransaction]] = Field(
        None, description="List of transactions in the statement"
    )
    others: Optional[Dict] = Field(
        None, description="Any other additional data from the statement"
    )


# If you don't pass an api key, it will look for the TENSORLAKE_API_KEY environment variable
doc_ai = DocumentAI()

# Use this already uploaded file for testing
file_id = "https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/510071197-TD-Bank-statement.pdf"

# If you want to upload your own file, uncomment the following lines:
# file_path = "path_to_your_file.pdf"
# file_id = doc_ai.upload(file_path)

# Configure parsing options
parsing_options = ParsingOptions(chunking_strategy=ChunkingStrategy.PAGE)

# Configure structured extraction options
structured_extraction_options = [
    StructuredExtractionOptions(
        schema_name="address",
        json_schema=Address,  # Can pass Pydantic model directly
    ),
    StructuredExtractionOptions(
        schema_name="bank transaction",
        json_schema=BankTransaction,
    ),
    StructuredExtractionOptions(
        schema_name="bank statement",
        json_schema=BankStatement,
    ),
]

# Parse the document
parse_id = doc_ai.parse(
    file_id,
    parsing_options=parsing_options,
    structured_extraction_options=structured_extraction_options,
)

# Wait for completion
result = doc_ai.wait_for_completion(parse_id=parse_id)

print(f"Parse status: {result.status}")

print("Structured Extraction Results:")
for structured_data in result.structured_data:
    print(json.dumps(structured_data.data, indent=2))
