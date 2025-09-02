import os
import unittest

from json_schemas.bank_statement import BankStatement

from tensorlake.documentai import DocumentAI
from tensorlake.documentai.models import (
    DatasetDataFilter,
    PageClassConfig,
    ParseStatus,
    StructuredExtractionOptions,
)


class TestDatasets(unittest.TestCase):
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

    def test_create_dataset(self):
        random_name = f"test_dataset_{os.urandom(4).hex()}"

        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing.",
        )

        self.assertIsNotNone(dataset)
        self.assertEqual(dataset.name, random_name)
        self.assertEqual(
            dataset.description, "This is a test dataset for unit testing."
        )
        self.assertEqual(dataset.status, "idle")
        self.assertIsNotNone(dataset.created_at)
        self.assertTrue(dataset.dataset_id.startswith("dataset_"))

        datasets_list = self.doc_ai.list_datasets()
        self.assertIsNotNone(datasets_list)
        self.assertGreater(len(datasets_list.items), 0)

        found_dataset = next(
            (d for d in datasets_list.items if d.dataset_id == dataset.dataset_id),
            None,
        )
        self.assertIsNotNone(found_dataset)
        self.assertEqual(found_dataset.name, random_name)

        self.doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            self.doc_ai.get_dataset,
            dataset.dataset_id,
        )

    def test_parse_documents(self):
        random_name = f"test_dataset_{os.urandom(4).hex()}"

        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing.",
        )
        self.assertIsNotNone(dataset)

        parse_id = self.doc_ai.parse_dataset_file(
            dataset=dataset,
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
            wait_for_completion=False,
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)

        dataset_data = self.doc_ai.get_dataset_data(dataset=dataset)
        self.assertIsNotNone(dataset_data)

        found_parse_id = next(
            (item.parse_id for item in dataset_data.items if item.parse_id == parse_id),
            None,
        )
        self.assertIsNotNone(found_parse_id)

        pending_dataset_data = self.doc_ai.get_dataset_data(
            dataset=dataset, filters=DatasetDataFilter(status=ParseStatus.PENDING)
        )
        self.assertIsNotNone(pending_dataset_data)
        self.assertEqual(len(pending_dataset_data.items), 0)

        self.doc_ai.delete_dataset(dataset)
        self.assertRaises(Exception, self.doc_ai.get_parsed_result, parse_id)

    def test_structured_extraction_dataset(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic", json_schema=BankStatement
        )

        random_name = f"test_dataset_{os.urandom(4).hex()}_structured_extraction"
        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing with structured extraction.",
            structured_extraction_options=[structured_extraction_options],
        )

        self.assertIsNotNone(dataset)

        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = self.doc_ai.parse_dataset_file(dataset=dataset, file=file_id)

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

        dataset_data = self.doc_ai.get_dataset_data(dataset=dataset)
        self.assertIsNotNone(dataset_data)

        found_parse_id = next(
            (item.parse_id for item in dataset_data.items if item.parse_id == parse_id),
            None,
        )
        self.assertIsNotNone(found_parse_id)

        self.doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            self.doc_ai.get_parsed_result,
            parse_id,
        )

    def test_page_classification_dataset(self):
        form125_page_class_config = PageClassConfig(
            name="form125",
            description="ACORD 125: Applicant Information Section — captures general insured information, business details, and contacts",
        )

        form140_page_class_config = PageClassConfig(
            name="form140",
            description="ACORD 140: Property Section — includes details about property coverage, location, valuation, and limit",
        )

        random_name = f"test_dataset_{os.urandom(4).hex()}_page_classification"
        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing with page classification.",
            page_classifications=[
                form125_page_class_config,
                form140_page_class_config,
            ],
        )

        self.assertIsNotNone(dataset)

        accord_file_id = self.doc_ai.upload(path="./document_ai/testdata/acord.pdf")
        self.assertIsNotNone(accord_file_id)

        parsed_result = self.doc_ai.parse_dataset_file(
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

        dataset_data = self.doc_ai.get_dataset_data(dataset=dataset)
        self.assertIsNotNone(dataset_data)

        found_parse_id = next(
            (
                item.parse_id
                for item in dataset_data.items
                if item.parse_id == parsed_result.parse_id
            ),
            None,
        )
        self.assertIsNotNone(found_parse_id)

        self.doc_ai.delete_dataset(dataset)

        self.assertRaises(
            Exception,
            self.doc_ai.get_parsed_result,
            parsed_result.parse_id,
        )

    def test_update_dataset(self):
        random_name = f"test_dataset_{os.urandom(4).hex()}"

        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing.",
        )
        self.assertIsNotNone(dataset)

        parse_id = self.doc_ai.parse_dataset_file(
            dataset=dataset,
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
            wait_for_completion=False,
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.pages)
        self.assertIsNotNone(parse_result.chunks)

        # Update dataset
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic", json_schema=BankStatement
        )

        updated_dataset = self.doc_ai.update_dataset(
            dataset=dataset,
            structured_extraction_options=[structured_extraction_options],
        )
        self.assertIsNotNone(updated_dataset)
        self.assertEqual(updated_dataset.name, random_name)

        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = self.doc_ai.parse_dataset_file(dataset=updated_dataset, file=file_id)

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

        self.doc_ai.delete_dataset(updated_dataset)
        self.assertRaises(
            Exception,
            self.doc_ai.get_parsed_result,
            parse_id,
        )

    def test_dataset_accepts_files_from_files_v2(self):
        file_id = os.getenv("FILES_V2_FILE_ID")
        if not file_id:
            self.skipTest("FILES_V2_FILE_ID environment variable is not set.")

        if not file_id.startswith("file_"):
            self.skipTest("FILES_V2_FILE_ID must start with 'file_'.")

        random_name = f"test_dataset_{os.urandom(4).hex()}"

        dataset = self.doc_ai.create_dataset(
            name=random_name,
            description="This is a test dataset for unit testing.",
        )
        self.assertIsNotNone(dataset)

        parse_id = self.doc_ai.parse_dataset_file(
            dataset=dataset,
            file=file_id,
            page_range="1-2",
            wait_for_completion=False,
        )
        self.assertIsNotNone(parse_id)

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parse_result)
        self.assertEqual(parse_result.status, ParseStatus.SUCCESSFUL)

        self.doc_ai.delete_dataset(dataset)
        self.assertRaises(Exception, self.doc_ai.get_parsed_result, parse_id)


# if __name__ == "__main__":
#     unittest.main()
