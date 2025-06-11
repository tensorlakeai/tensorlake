import time
from datetime import date
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from tensorlake.documentai import DocumentAI, ExtractionOptions, ParsingOptions

API_KEY = "tl_apiKey_XXXX"

# Or set this in the environment variable TENSORLAKE_API_KEY


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


# if you don't pass an api key, it will look for the TENSORLAKE_API_KEY environment variable
doc_ai = DocumentAI(api_key=API_KEY)
# Skip this if you are passing a pre-signed URL to the `DocumentParser`.
# or pass an external URL
file_id = doc_ai.upload(path="./examples/documents/example_bank_statement.pdf")

job_id = doc_ai.parse(
    file_id,
    options=ParsingOptions(extraction_options=ExtractionOptions(schema=BankStatement)),
)

result = doc_ai.get_job(job_id=job_id)
print(f"job status: {result.status}")
while True:
    if result.status in ["processing", "pending"]:
        print("waiting 5s...")
        time.sleep(5)
        result = doc_ai.get_job(job_id)
        print(f"job status: {result.status}")
    else:
        if result.status == "successful":
            print(result)
        break
