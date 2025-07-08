import unittest
import os

from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.models.options import StructuredExtractionOptions

from json_schemas.bank_statement import BankStatement


class TestDatasets(unittest.TestCase):
    def test_create_dataset(self):
        os.environ["TENSORLAKE_API_KEY"] = "DEV_API_KEY"
        os.environ["INDEXIFY_URL"] = "https://api.tensorlake.dev"

        doc_ai = DocumentAI(
            server_url="https://api.tensorlake.dev",
            api_key="DEV_API_KEY",
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
        self.assertTrue(dataset.slug.startswith("test-dataset"))

        doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            doc_ai.get_dataset,
            dataset.slug,
        )

    def test_parse_documents(self):
        os.environ["TENSORLAKE_API_KEY"] = "DEV_API_KEY"
        os.environ["INDEXIFY_URL"] = "https://api.tensorlake.dev"

        doc_ai = DocumentAI(
            server_url="https://api.tensorlake.dev",
            api_key="DEV_API_KEY",
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
        self.assertIsNotNone(parse_result.document_layout)
        self.assertIsNotNone(parse_result.chunks)

        doc_ai.delete_dataset(dataset)
        self.assertRaises(Exception, doc_ai.get_parsed_result, parse_id)

    def test_structured_extraction_dataset(self):
        os.environ["TENSORLAKE_API_KEY"] = "DEV_API_KEY"
        os.environ["INDEXIFY_URL"] = "https://api.tensorlake.dev"

        doc_ai = DocumentAI(
            server_url="https://api.tensorlake.dev",
            api_key="DEV_API_KEY",
        )

        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic", json_schema=BankStatement
        )

        dataset = doc_ai.create_dataset(
            name="Test Dataset with Structured Extraction",
            description="This is a test dataset for unit testing with structured extraction.",
            structured_extraction_options=[structured_extraction_options],
        )

        self.assertIsNotNone(dataset)

        file_id = doc_ai.upload(path="./testdata/example_bank_statement.pdf")
        self.assertIsNotNone(file_id)

        parse_id = doc_ai.parse_dataset_file(dataset=dataset, file=file_id)

        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)

        self.assertIsNotNone(parse_result.document_layout)
        self.assertIsNotNone(parse_result.chunks)
        self.assertIsNotNone(parse_result.structured_data)

        self.assertIsNotNone(parse_result.structured_data.get("form125-basic"))

        doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            doc_ai.get_parsed_result,
            parse_id,
        )

    def test_list_datasets(self):
        # List the datasets
        # Assert the datasets are listed
        pass


if __name__ == "__main__":
    unittest.main()
