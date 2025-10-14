import os
import unittest

from tensorlake.documentai import DocumentAI


class TestFiles(unittest.TestCase):
    def setUp(self):
        server_url = os.getenv("TENSORLAKE_API_URL")
        self.assertIsNotNone(
            server_url, "TENSORLAKE_API_URL environment variable is not set."
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

    def test_uploads_small_file(self):
        file_id = self.doc_ai.upload(
            "./document_ai/testdata/example_bank_statement.pdf",
        )
        self.assertIsNotNone(file_id)

    def test_uploads_larger_files_than_10_mb(self):
        file_id = self.doc_ai.upload(
            "./document_ai/testdata/synthetic_20MB.csv",
        )
        self.assertIsNotNone(file_id)


if __name__ == "__main__":
    unittest.main()
