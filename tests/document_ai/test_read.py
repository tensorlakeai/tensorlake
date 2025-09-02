import os
import unittest

from tensorlake.documentai import (
    DocumentAI,
    ParseStatus,
)


class TestRead(unittest.TestCase):
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

    def test_read(self):
        parse_id = self.doc_ai.read(
            file_url="https://raw.githubusercontent.com/tensorlakeai/tensorlake/main/tests/document_ai/testdata/example_bank_statement.pdf",
            page_range="1-2",
        )
        self.assertIsNotNone(parse_id)
        print(f"Parse read ID: {parse_id}")

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

        if parse_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parse_id)
            self.assertRaises(Exception, self.doc_ai.get_parsed_result, parse_id)


if __name__ == "__main__":
    unittest.main()
