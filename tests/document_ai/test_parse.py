import os
import unittest

from json_schemas.bank_statement import BankStatement

from tensorlake.documentai import (
    DocumentAI,
)
from tensorlake.documentai.models import (
    PageClassConfig,
    ParseStatus,
    StructuredExtractionOptions,
)


class TestParse(unittest.TestCase):
    def setUp(self):
        server_url = os.getenv("INDEXIFY_URL")
        self.assertIsNotNone(
            server_url, "INDEXIFY_URL environment variable is not set."
        )

        api_key = os.getenv("TENSORLAKE_API_KEY")
        self.assertIsNotNone(
            api_key, "TENSORLAKE_API_KEY environment variable is not set."
        )

        self.doc_ai = DocumentAI(
            server_url=server_url,
            api_key=api_key,
        )
        self.addCleanup(self.doc_ai.close)

    def test_simple_parse(self):
        parse_id = self.doc_ai.parse(
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertEqual(parse_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)

        parse_list = self.doc_ai.list_parse_results()
        self.assertIsNotNone(parse_list)
        self.assertGreater(len(parse_list.items), 0)

        found_parse = next(
            (p for p in parse_list.items if p.parse_id == parse_id), None
        )
        self.assertIsNotNone(found_parse)
        self.assertEqual(found_parse.parse_id, parse_id)

        self.doc_ai.delete_parse(parse_id)
        self.assertRaises(Exception, self.doc_ai.get_parsed_result, parse_id)

    def test_remove_file_can_still_access_parsed_results(self):
        file_id = self.doc_ai.upload(
            "./document_ai/testdata/example_bank_statement.pdf",
        )
        self.assertIsNotNone(file_id)

        parsed_result = self.doc_ai.parse_and_wait(
            file=file_id,
            page_range="1",
        )
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parsed_result.pages)

        self.doc_ai.delete_file(file_id)

        # After deleting the file, we should still be able to access the parsed results
        parsed_result = self.doc_ai.get_parsed_result(parsed_result.parse_id)
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parsed_result.pages)

        self.doc_ai.delete_parse(parsed_result.parse_id)
        self.assertRaises(
            Exception, self.doc_ai.get_parsed_result, parsed_result.parse_id
        )

    def test_parse_structured_extraction(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic", json_schema=BankStatement
        )

        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = self.doc_ai.parse(
            file=file_id, structured_extraction_options=[structured_extraction_options]
        )

        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)

        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)
        self.assertIsNotNone(parse_result.structured_data)

        structured_extraction_schemas = {}
        for schema in parse_result.structured_data:
            structured_extraction_schemas[schema.schema_name] = schema

        self.assertIsNotNone(structured_extraction_schemas.get("form125-basic"))

        self.doc_ai.delete_parse(parse_result.parse_id)
        self.assertRaises(
            Exception, self.doc_ai.get_parsed_result, parse_result.parse_id
        )

    def test_page_classification(self):
        form125_page_class_config = PageClassConfig(
            name="form125",
            description="ACORD 125: Applicant Information Section — captures general insured information, business details, and contacts",
        )

        form140_page_class_config = PageClassConfig(
            name="form140",
            description="ACORD 140: Property Section — includes details about property coverage, location, valuation, and limit",
        )

        accord_file_id = self.doc_ai.upload(path="./document_ai/testdata/acord.pdf")
        self.assertIsNotNone(accord_file_id)

        parsed_result = self.doc_ai.parse_and_wait(
            file=accord_file_id,
            page_classifications=[form125_page_class_config, form140_page_class_config],
        )
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)

        self.assertIsNotNone(parsed_result.pages)
        self.assertIsNotNone(parsed_result.page_classes)
        self.assertEqual(
            len(parsed_result.page_classes), 2, "Expected two page classes"
        )

        page_classes = {}
        for pc in parsed_result.page_classes:
            page_classes[pc.page_class] = pc

        self.assertIn("form125", page_classes)
        self.assertIn("form140", page_classes)

        self.doc_ai.delete_parse(parsed_result.parse_id)
        self.assertRaises(
            Exception, self.doc_ai.get_parsed_result, parsed_result.parse_id
        )

    def test_parse_structured_extraction_dict_json_schema(self):
        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        bank_statement_schema: dict = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "BankStatementData",
            "type": "object",
            "properties": {
                "accountHolder": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "description": "Full name of the account holder",
                        },
                        "address": {
                            "type": "string",
                            "description": "Address of the account holder",
                        },
                    },
                    "required": ["name"],
                },
                "accountInfo": {
                    "type": "object",
                    "properties": {
                        "accountNumber": {
                            "type": "string",
                            "description": "Account number",
                        },
                        "sortCode": {
                            "type": "string",
                            "description": "Sort code (UK format)",
                        },
                        "accountType": {
                            "type": "string",
                            "description": "Type of account (e.g., Current, Savings)",
                        },
                        "statementPeriod": {
                            "type": "object",
                            "properties": {
                                "startDate": {
                                    "type": "string",
                                    "description": "Start of the statement period (YYYY-MM-DD)",
                                },
                                "endDate": {
                                    "type": "string",
                                    "description": "End of the statement period (YYYY-MM-DD)",
                                },
                            },
                        },
                    },
                    "required": ["accountNumber"],
                },
                "bankInfo": {
                    "type": "object",
                    "properties": {
                        "bankName": {
                            "type": "string",
                            "description": "Name of the bank",
                        },
                        "branchAddress": {
                            "type": "string",
                            "description": "Branch address",
                        },
                    },
                },
                "balanceSummary": {
                    "type": "object",
                    "properties": {
                        "openingBalance": {
                            "type": "number",
                            "description": "Balance at the start of the period",
                        },
                        "closingBalance": {
                            "type": "number",
                            "description": "Balance at the end of the period",
                        },
                        "totalCredits": {
                            "type": "number",
                            "description": "Total money received in the period",
                        },
                        "totalDebits": {
                            "type": "number",
                            "description": "Total money spent in the period",
                        },
                    },
                },
                "transactions": {
                    "type": "array",
                    "description": "All transactions in the statement period",
                    "items": {
                        "type": "object",
                        "properties": {
                            "date": {
                                "type": "string",
                                "description": "Transaction date (YYYY-MM-DD)",
                            },
                            "description": {
                                "type": "string",
                                "description": "Transaction description or reference",
                            },
                            "amount": {
                                "type": "number",
                                "description": "Amount (credit, debit)",
                            },
                            "type": {
                                "type": "string",
                                "enum": ["credit", "debit"],
                                "description": "Transaction type",
                            },
                            "balance": {
                                "type": "number",
                                "description": "Balance after this transaction",
                            },
                            "reference": {
                                "type": "string",
                                "description": "Transaction reference number",
                            },
                        },
                        "required": ["date", "description", "amount", "type"],
                    },
                },
                "metadata": {
                    "type": "object",
                    "properties": {
                        "statementDate": {
                            "type": "string",
                            "description": "Date the statement was generated",
                        },
                        "pageCount": {
                            "type": "integer",
                            "description": "Number of pages in the statement",
                        },
                        "extractionMethod": {
                            "type": "string",
                            "description": "Extraction method (e.g., pdfplumber, vision-api)",
                        },
                    },
                },
            },
            "required": ["accountHolder", "accountInfo", "transactions"],
        }

        bank_statement_extraction_options = StructuredExtractionOptions(
            schema_name="BankStatementData",
            json_schema=bank_statement_schema,
        )

        result = self.doc_ai.parse_and_wait(
            file_id,
            structured_extraction_options=[bank_statement_extraction_options],
        )

        self.assertIsNotNone(result)
        self.assertIsNotNone(result.structured_data)
        self.assertEqual(len(result.structured_data), 1)
        self.assertEqual(result.structured_data[0].schema_name, "BankStatementData")

        self.doc_ai.delete_parse(result.parse_id)
        self.assertRaises(Exception, self.doc_ai.get_parsed_result, result.parse_id)


if __name__ == "__main__":
    unittest.main()
