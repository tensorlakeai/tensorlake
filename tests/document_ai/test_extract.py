import os
import unittest

from json_schemas.bank_statement import BankStatement

from tensorlake.documentai import (
    DocumentAI,
    ParseStatus,
    PartitionStrategy,
    StructuredExtractionOptions,
)


class TestExtract(unittest.TestCase):
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

    def test_extract(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic",
            json_schema=BankStatement,
            partition_strategy=PartitionStrategy.PAGE,
        )

        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = self.doc_ai.extract(
            structured_extraction_options=[structured_extraction_options],
            file_id=file_id,
        )

        self.assertIsNotNone(parse_id)
        print(f"Extract ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)

        self.assertEqual(parse_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.structured_data)

        structured_extraction_schemas = {}
        for schema in parse_result.structured_data or []:
            structured_extraction_schemas[schema.schema_name] = schema

        self.assertIsNotNone(structured_extraction_schemas.get("form125-basic"))

        if parse_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parse_result.parse_id)
            self.assertRaises(
                Exception, self.doc_ai.get_parsed_result, parse_result.parse_id
            )

    def test_extract_partition_with_patterns(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="form125-basic",
            json_schema=BankStatement,
            partition_strategy={
                "strategy": "patterns",
                "start_patterns": ["Account Summary"],
                "end_patterns": ["End of Statement"],
            },
        )

        file_id = self.doc_ai.upload(
            path="./document_ai/testdata/example_bank_statement.pdf"
        )
        self.assertIsNotNone(file_id)

        parse_id = self.doc_ai.extract(
            structured_extraction_options=[structured_extraction_options],
            file_id=file_id,
        )

        self.assertIsNotNone(parse_id)
        print(f"Extract ID: {parse_id}")

        parse_result = self.doc_ai.wait_for_completion(parse_id=parse_id)

        self.assertEqual(parse_result.status, ParseStatus.SUCCESSFUL)
        self.assertIsNotNone(parse_result)
        self.assertIsNotNone(parse_result.structured_data)

        structured_extraction_schemas = {}
        for schema in parse_result.structured_data or []:
            structured_extraction_schemas[schema.schema_name] = schema

        self.assertIsNotNone(structured_extraction_schemas.get("form125-basic"))

        if parse_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parse_result.parse_id)
            self.assertRaises(
                Exception, self.doc_ai.get_parsed_result, parse_result.parse_id
            )


if __name__ == "__main__":
    unittest.main()
