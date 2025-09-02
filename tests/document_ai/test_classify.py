import os
import unittest

from tensorlake.documentai import DocumentAI, PageClassConfig, ParseStatus


class TestClassify(unittest.TestCase):
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

    def test_classify(self):
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

        parse_id = self.doc_ai.classify(
            page_classifications=[form125_page_class_config, form140_page_class_config],
            file_id=accord_file_id,
        )
        self.assertIsNotNone(parse_id)
        print(f"Classify Parse ID: {parse_id}")

        parsed_result = self.doc_ai.wait_for_completion(parse_id=parse_id)
        self.assertIsNotNone(parsed_result)
        self.assertEqual(parsed_result.status, ParseStatus.SUCCESSFUL)

        self.assertIsNotNone(parsed_result.page_classes)
        self.assertEqual(
            len(parsed_result.page_classes), 2, "Expected two page classes"
        )

        page_classes = {}
        for pc in parsed_result.page_classes or []:
            page_classes[pc.page_class] = pc

        self.assertIn("form125", page_classes)
        self.assertIn("form140", page_classes)

        if parsed_result.status == ParseStatus.SUCCESSFUL:
            self.doc_ai.delete_parse(parsed_result.parse_id)
            self.assertRaises(
                Exception, self.doc_ai.get_parsed_result, parsed_result.parse_id
            )


if __name__ == "__main__":
    unittest.main()
