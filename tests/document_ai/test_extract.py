import os
import unittest
from pathlib import Path

from tensorlake.documentai import (
    DocumentAI,
    ParseStatus,
    PartitionStrategy,
    PatternConfig,
    PatternPartitionStrategy,
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

        self.test_dir = Path(__file__).parent
        self.test_data_dir = self.test_dir / "testdata"

    def test_extract(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="w2FormSimple",
            json_schema={
                "title": "w2FormSimple",
                "type": "object",
                "properties": {
                    "ssn": {
                        "type": "string",
                        "description": "Employee's Social Security Number (Box a)",
                    },
                    "employerName": {
                        "type": "string",
                        "description": "Full name of the employer (Box c)",
                    },
                    "wagesTipsOtherCompensation": {
                        "type": "number",
                        "description": "Wages, tips, and other compensation (Box 1)",
                    },
                },
                "required": ["ssn", "employerName", "wagesTipsOtherCompensation"],
            },
            partition_strategy=PartitionStrategy.SECTION,
        )

        test_file_path = self.test_data_dir / "w2.pdf"
        file_id = self.doc_ai.upload(path=str(test_file_path.absolute()))
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

        self.assertIsNotNone(structured_extraction_schemas.get("w2FormSimple"))

        if parse_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parse_result.parse_id)
            self.assertRaises(
                Exception, self.doc_ai.get_parsed_result, parse_result.parse_id
            )

    def test_extract_partition_with_patterns(self):
        structured_extraction_options = StructuredExtractionOptions(
            schema_name="w2FormSimple",
            json_schema={
                "title": "w2FormSimple",
                "type": "object",
                "properties": {
                    "ssn": {
                        "type": "string",
                        "description": "Employee's Social Security Number (Box a)",
                    },
                    "employerName": {
                        "type": "string",
                        "description": "Full name of the employer (Box c)",
                    },
                    "wagesTipsOtherCompensation": {
                        "type": "number",
                        "description": "Wages, tips, and other compensation (Box 1)",
                    },
                },
                "required": ["ssn", "employerName", "wagesTipsOtherCompensation"],
            },
            partition_strategy=PatternPartitionStrategy(
                patterns=PatternConfig(
                    start_patterns=[r"Form W-2", r"Employee's social security number"],
                    end_patterns=[
                        r"Department of the Treasury",
                        r"Employer identification number",
                    ],
                )
            ),
        )

        test_file_path = self.test_data_dir / "w2.pdf"
        file_id = self.doc_ai.upload(path=str(test_file_path.absolute()))
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

        self.assertIsNotNone(structured_extraction_schemas.get("w2FormSimple"))

        if parse_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parse_result.parse_id)
            self.assertRaises(
                Exception, self.doc_ai.get_parsed_result, parse_result.parse_id
            )

    def test_extract_invalid_patterns_partition_strategy(self):
        with self.assertRaises(ValueError) as context:
            StructuredExtractionOptions(
                schema_name="w2FormSimple",
                json_schema={
                    "title": "w2FormSimple",
                    "type": "object",
                    "properties": {
                        "ssn": {
                            "type": "string",
                            "description": "Employee's Social Security Number (Box a)",
                        },
                        "employerName": {
                            "type": "string",
                            "description": "Full name of the employer (Box c)",
                        },
                        "wagesTipsOtherCompensation": {
                            "type": "number",
                            "description": "Wages, tips, and other compensation (Box 1)",
                        },
                    },
                    "required": ["ssn", "employerName", "wagesTipsOtherCompensation"],
                },
                partition_strategy=PatternPartitionStrategy(
                    patterns=PatternConfig(
                        start_patterns=None,
                        end_patterns=None,
                    )
                ),
            )

        self.assertIn(
            "At least one start or end pattern must be provided.",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main()
