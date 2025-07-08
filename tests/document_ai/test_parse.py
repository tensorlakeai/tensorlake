import unittest
import os

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models.enums import ParseStatus
from tensorlake.documentai.models.options import (
    StructuredExtractionOptions,
    PageClassConfig,
)

from json_schemas.bank_statement import BankStatement


class TestParse(unittest.TestCase):

    def test_simple_parse(self):
        server_url = os.getenv("INDEXIFY_URL")
        self.assertIsNotNone(
            server_url, "INDEXIFY_URL environment variable is not set."
        )

        api_key = os.getenv("TENSORLAKE_API_KEY")
        self.assertIsNotNone(
            api_key, "TENSORLAKE_API_KEY environment variable is not set."
        )

        doc_ai = DocumentAI(
            server_url=server_url,
            api_key=api_key,
        )

        parse_id = doc_ai.parse(
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.document_layout)
        self.assertIsNotNone(parse_result.chunks)

    def test_remove_file_can_still_access_parsed_results(self):
        server_url = os.getenv("INDEXIFY_URL")
        self.assertIsNotNone(
            server_url, "INDEXIFY_URL environment variable is not set."
        )

        api_key = os.getenv("TENSORLAKE_API_KEY")
        self.assertIsNotNone(
            api_key, "TENSORLAKE_API_KEY environment variable is not set."
        )

        doc_ai = DocumentAI(
            server_url=server_url,
            api_key=api_key,
        )

        file_id = doc_ai.upload(
            "./testdata/example_bank_statement.pdf",
        )
        self.assertIsNotNone(file_id)

        parsed_result = doc_ai.parse_and_wait(
            file=file_id,
            page_range="1",
        )
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parsed_result.document_layout)

        doc_ai.delete_file(file_id)

        # After deleting the file, we should still be able to access the parsed results
        parsed_result = doc_ai.get_parsed_result(parsed_result.parse_id)
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parsed_result.document_layout)

    def test_parse_structured_extraction(self):
        server_url = os.getenv("INDEXIFY_URL")
        self.assertIsNotNone(
            server_url, "INDEXIFY_URL environment variable is not set."
        )

        api_key = os.getenv("TENSORLAKE_API_KEY")
        self.assertIsNotNone(
            api_key, "TENSORLAKE_API_KEY environment variable is not set."
        )

        doc_ai = DocumentAI(server_url=server_url, api_key=api_key)
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic", json_schema=BankStatement
        )

        file_id = doc_ai.upload(
            path="./tests/document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = doc_ai.parse(
            file=file_id, structured_extraction_options=[structured_extraction_options]
        )

        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)

        self.assertIsNotNone(parse_result.document_layout)
        self.assertIsNotNone(parse_result.chunks)
        self.assertIsNotNone(parse_result.structured_data)

        self.assertIsNotNone(parse_result.structured_data.get("form125-basic"))

    def test_page_classification(self):
        server_url = os.getenv("INDEXIFY_URL")
        self.assertIsNotNone(
            server_url, "INDEXIFY_URL environment variable is not set."
        )

        api_key = os.getenv("TENSORLAKE_API_KEY")
        self.assertIsNotNone(
            api_key, "TENSORLAKE_API_KEY environment variable is not set."
        )

        doc_ai = DocumentAI(
            server_url=server_url,
            api_key=api_key,
        )

        form125_page_class_config = PageClassConfig(
            name="form125",
            description="ACORD 125: Applicant Information Section — captures general insured information, business details, and contacts",
        )

        form140_page_class_config = PageClassConfig(
            name="form140",
            description="ACORD 140: Property Section — includes details about property coverage, location, valuation, and limit",
        )

        accord_file_id = doc_ai.upload(path="./document_ai/testdata/acord.pdf")
        self.assertIsNotNone(accord_file_id)

        parsed_result = doc_ai.parse_and_wait(
            file=accord_file_id,
            page_classifications=[form125_page_class_config, form140_page_class_config],
        )
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)

        self.assertIsNotNone(parsed_result.document_layout)
        self.assertIsNotNone(parsed_result.page_classes)
        self.assertEqual(
            len(parsed_result.page_classes), 2, "Expected two page classes"
        )
        self.assertIn("form125", parsed_result.page_classes)
        self.assertIn("form140", parsed_result.page_classes)


if __name__ == "__main__":
    unittest.main()
