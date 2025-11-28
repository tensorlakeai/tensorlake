import io
import json
import sys
import unittest
from datetime import datetime, timezone

from tensorlake.applications import SerializationError
from tensorlake.applications.cloud_events import (
    new_cloud_event,
    print_cloud_event,
)


class TestCloudEventStructure(unittest.TestCase):
    """Tests for CloudEvent structure and CloudEvents 1.0 spec compliance."""

    def test_new_cloud_event_creates_valid_structure(self):
        """Test that new_cloud_event creates a valid CloudEvent structure."""
        event_data = {"test": "data"}
        event = new_cloud_event(event_data)

        # Verify required CloudEvents 1.0 spec fields
        self.assertIn("specversion", event)
        self.assertEqual(event["specversion"], "1.0")
        self.assertIn("id", event)
        self.assertIn("timestamp", event)
        self.assertIn("type", event)
        self.assertIn("source", event)
        self.assertIn("data", event)

        # Verify data is preserved
        self.assertEqual(event["data"], event_data)

    def test_new_cloud_event_default_type(self):
        """Test that new_cloud_event uses default type when not specified."""
        event = new_cloud_event({"test": "data"})
        self.assertEqual(event["type"], "ai.tensorlake.event")

    def test_new_cloud_event_custom_type(self):
        """Test that new_cloud_event respects custom type."""
        event = new_cloud_event({"test": "data"}, type="custom.event.type")
        self.assertEqual(event["type"], "custom.event.type")

    def test_new_cloud_event_default_source(self):
        """Test that new_cloud_event uses default source when not specified."""
        event = new_cloud_event({"test": "data"})
        self.assertEqual(event["source"], "/tensorlake/function_executor/events")

    def test_new_cloud_event_custom_source(self):
        """Test that new_cloud_event respects custom source."""
        event = new_cloud_event({"test": "data"}, source="/custom/source/path")
        self.assertEqual(event["source"], "/custom/source/path")

    def test_new_cloud_event_without_message(self):
        """Test that message field is not included when not provided."""
        event = new_cloud_event({"test": "data"})
        self.assertNotIn("message", event)

    def test_new_cloud_event_with_message(self):
        """Test that message field is included when provided."""
        event = new_cloud_event({"test": "data"}, message="test message")
        self.assertIn("message", event)
        self.assertEqual(event["message"], "test message")

    def test_new_cloud_event_unique_ids(self):
        """Test that each CloudEvent gets a unique ID."""
        event1 = new_cloud_event({"data": 1})
        event2 = new_cloud_event({"data": 2})

        self.assertNotEqual(event1["id"], event2["id"])

    def test_new_cloud_event_different_timestamps(self):
        """Test that CloudEvents created at different times have different timestamps."""
        event1 = new_cloud_event({"data": 1})
        # Small delay to ensure different timestamps
        import time

        time.sleep(0.001)
        event2 = new_cloud_event({"data": 2})

        self.assertNotEqual(event1["timestamp"], event2["timestamp"])


class TestCloudEventTimestamp(unittest.TestCase):
    """Tests for CloudEvent timestamp handling."""

    def test_timestamp_is_iso_format(self):
        """Test that timestamp is in ISO 8601 format."""
        event = new_cloud_event({"test": "data"})
        timestamp = event["timestamp"]

        # Should match ISO 8601 format with UTC timezone (Z)
        # Format: YYYY-MM-DDTHH:MM:SS.ffffffZ
        iso_pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+\+00:00"
        self.assertRegex(timestamp, iso_pattern)

    def test_timestamp_ends_with_z(self):
        """Test that timestamp ends with +00:00 indicating UTC timezone."""
        event = new_cloud_event({"test": "data"})
        self.assertTrue(event["timestamp"].endswith("+00:00"))

    def test_timestamp_is_parseable(self):
        """Test that timestamp can be parsed as valid ISO 8601."""
        event = new_cloud_event({"test": "data"})
        timestamp = event["timestamp"]

        try:
            parsed = datetime.fromisoformat(timestamp)
            self.assertEqual(parsed.tzinfo, timezone.utc)
        except ValueError as e:
            self.fail(f"Timestamp could not be parsed: {timestamp}, error: {e}")


class TestCloudEventSerialization(unittest.TestCase):
    """Tests for CloudEvent JSON serialization."""

    def test_new_cloud_event_is_json_serializable(self):
        """Test that CloudEvent can be serialized to JSON."""
        event = new_cloud_event({"key": "value"})
        json_str = json.dumps(event)
        self.assertIsNotNone(json_str)

        # Verify we can deserialize it back
        deserialized = json.loads(json_str)
        self.assertEqual(deserialized["type"], event["type"])
        self.assertEqual(deserialized["source"], event["source"])
        self.assertEqual(deserialized["data"], event["data"])

    def test_cloud_event_with_complex_data(self):
        """Test CloudEvent serialization with complex nested data."""
        event_data = {
            "nested": {
                "list": [1, 2, 3],
                "dict": {"key": "value"},
                "number": 42,
                "float": 3.14,
                "bool": True,
                "null": None,
            }
        }
        event = new_cloud_event(event_data)
        json_str = json.dumps(event)
        deserialized = json.loads(json_str)

        self.assertEqual(deserialized["data"], event_data)

    def test_cloud_event_with_special_characters(self):
        """Test CloudEvent serialization with special characters."""
        event_data = {
            "message": 'Test with "quotes" and \\ backslashes and \n newlines',
            "unicode": "测试中文 🎉",
        }
        event = new_cloud_event(event_data)
        json_str = json.dumps(event)
        deserialized = json.loads(json_str)

        self.assertEqual(deserialized["data"]["message"], event_data["message"])
        self.assertEqual(deserialized["data"]["unicode"], event_data["unicode"])


class TestPrintCloudEvent(unittest.TestCase):
    """Tests for print_cloud_event function."""

    def setUp(self):
        """Capture stdout before each test."""
        self.captured_output = io.StringIO()
        sys.stdout = self.captured_output

    def tearDown(self):
        """Restore stdout after each test."""
        sys.stdout = sys.__stdout__

    def test_print_cloud_event_outputs_json(self):
        """Test that print_cloud_event outputs valid JSON."""
        event_data = {"test": "data"}
        print_cloud_event(event_data)

        output = self.captured_output.getvalue().strip().split("\n")
        parsed = json.loads(output[0])

        # Verify it's a valid CloudEvent
        self.assertEqual(parsed["specversion"], "1.0")
        self.assertEqual(parsed["type"], "ai.tensorlake.event")
        self.assertEqual(parsed["data"], event_data)

    def test_print_cloud_event_with_custom_parameters(self):
        """Test print_cloud_event with custom type, source, and message."""
        event_data = {"key": "value"}
        print_cloud_event(
            event_data,
            type="custom.type",
            source="/custom/source",
            message="test message",
        )

        output = self.captured_output.getvalue().strip().split("\n")
        parsed = json.loads(output[0])

        self.assertEqual(parsed["type"], "custom.type")
        self.assertEqual(parsed["source"], "/custom/source")
        self.assertEqual(parsed["message"], "test message")

    def test_print_cloud_event_invalid_data_raises_error(self):
        """Test that non-serializable data raises SerializationError."""

        # Create an object that can't be JSON serialized
        class NonSerializable:
            pass

        event_data = {"obj": NonSerializable()}

        with self.assertRaises(SerializationError) as context:
            print_cloud_event(event_data)

        self.assertIn(
            "Failed to serialize event payload: Object of type NonSerializable is not JSON serializable",
            str(context.exception),
        )


if __name__ == "__main__":
    unittest.main()
