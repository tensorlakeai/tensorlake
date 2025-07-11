import os
import unittest

from json_schemas.bank_statement import BankStatement

from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.models.enums import ParseStatus
from tensorlake.documentai.models.options import (
    PageClassConfig,
    StructuredExtractionOptions,
)


class TestDatasets(unittest.TestCase):
    def test_create_dataset(self):
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

        dataset = doc_ai.create_dataset(
            name="Test Dataset",
            description="This is a test dataset for unit testing.",
        )

        self.assertIsNotNone(dataset)
        self.assertEqual(dataset.name, "Test Dataset")
        self.assertEqual(
            dataset.description, "This is a test dataset for unit testing."
        )
        self.assertEqual(dataset.status, "idle")
        self.assertIsNotNone(dataset.created_at)
        self.assertTrue(dataset.dataset_id.startswith("dataset_"))

        doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            doc_ai.get_dataset,
            dataset.dataset_id,
        )

    def test_parse_documents(self):
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

        dataset = doc_ai.create_dataset(
            name="Test Dataset",
            description="This is a test dataset for unit testing.",
        )
        self.assertIsNotNone(dataset)

        parse_id = doc_ai.parse_dataset_file(
            dataset=dataset,
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
            wait_for_completion=False,
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")
        parse_result = doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)

        doc_ai.delete_dataset(dataset)
        self.assertRaises(Exception, doc_ai.get_parsed_result, parse_id)

    def test_structured_extraction_dataset(self):
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

        dataset = doc_ai.create_dataset(
            name="Test Dataset with Structured Extraction",
            description="This is a test dataset for unit testing with structured extraction.",
            structured_extraction_options=[structured_extraction_options],
        )

        self.assertIsNotNone(dataset)

        file_id = doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = doc_ai.parse_dataset_file(dataset=dataset, file=file_id)

        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)

        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)
        self.assertIsNotNone(parse_result.structured_data)

        structured_extraction_schemas = {}
        for schema in parse_result.structured_data:
            structured_extraction_schemas[schema.schema_name] = schema

        self.assertIsNotNone(structured_extraction_schemas.get("form125-basic"))

        doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            doc_ai.get_parsed_result,
            parse_id,
        )

    def test_page_classification_dataset(self):
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

        dataset = doc_ai.create_dataset(
            name="Test Dataset with Page Classification",
            description="This is a test dataset for unit testing with page classification.",
            page_classifications=[
                form125_page_class_config,
                form140_page_class_config,
            ],
        )

        self.assertIsNotNone(dataset)

        accord_file_id = doc_ai.upload(path="./document_ai/testdata/acord.pdf")
        self.assertIsNotNone(accord_file_id)

        parsed_result = doc_ai.parse_dataset_file(
            dataset=dataset,
            file=accord_file_id,
            wait_for_completion=True,
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


# if __name__ == "__main__":
#     unittest.main()
