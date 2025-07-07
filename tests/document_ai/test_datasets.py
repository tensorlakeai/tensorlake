import unittest
import os

from tensorlake.documentai.client import DocumentAI


class TestDatasets(unittest.TestCase):
    def test_create_dataset(self):
        os.environ["TENSORLAKE_API_KEY"] = ""
        os.environ["INDEXIFY_URL"] = "https://api.tensorlake.dev"

        doc_ai = DocumentAI(server_url="https://api.tensorlake.dev")

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
        self.assertEqual(dataset.slug, "test-dataset")

        doc_ai.delete_dataset(dataset.slug)

        get_dataset = doc_ai.get_dataset(dataset.slug)
        self.assertIsNone(get_dataset)

    def test_parse_documents(self):
        os.environ["TENSORLAKE_API_KEY"] = ""
        os.environ["INDEXIFY_URL"] = "https://api.tensorlake.dev"

        doc_ai = DocumentAI(server_url="https://api.tensorlake.dev")

        dataset = doc_ai.create_dataset(
            name="Test Dataset",
            description="This is a test dataset for unit testing.",
        )

        parse_result = doc_ai.parse_dataset_file(
            dataset=dataset,
            file="https://pub-226479de18b2493f96b64c6674705dd8.r2.dev/real-estate-purchase-all-signed.pdf",
            page_range="1-2",
            wait_for_completion=True,
        )

        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.document)

        doc_ai.delete_dataset(dataset.slug)

    def test_list_datasets(self):
        # List the datasets
        # Assert the datasets are listed
        pass


if __name__ == "__main__":
    unittest.main()
