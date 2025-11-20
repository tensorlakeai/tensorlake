import unittest

from pydantic import BaseModel

from tensorlake.applications import DeserializationError
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    serialize_value,
)
from tensorlake.applications.metadata import ValueMetadata
from tensorlake.applications.user_data_serializer import (
    JSONUserDataSerializer,
    PickleUserDataSerializer,
)


class TestPydanticModel(BaseModel):
    name: str
    age: int


class ModelA(BaseModel):
    value: str


class ModelB(BaseModel):
    value: int


class TestModel(BaseModel):
    name: str


class TestUserDataSerializer(unittest.TestCase):
    """Tests for user data serializers."""

    def test_serialize_deserialize_basic_types_json(self):
        """Test serialization and deserialization of basic Python types with JSON serializer."""
        serializer = JSONUserDataSerializer()
        test_values = [
            "David",  # The critical test case
            42,
            3.14,
            True,
            None,
            [1, 2, 3],
            {"key": "value"},
        ]

        for value in test_values:
            data, metadata = serialize_value(
                value, serializer, f"test_{type(value).__name__}"
            )
            result = deserialize_value(data, metadata)
            self.assertEqual(result, value)
            self.assertEqual(type(result), type(value))

    def test_serialize_deserialize_basic_types_pickle(self):
        """Test serialization and deserialization of basic Python types with Pickle serializer."""
        serializer = PickleUserDataSerializer()
        test_values = [
            "David",  # The critical test case
            42,
            3.14,
            True,
            None,
            [1, 2, 3],
            {"key": "value"},
        ]

        for value in test_values:
            data, metadata = serialize_value(
                value, serializer, f"test_{type(value).__name__}"
            )
            result = deserialize_value(data, metadata)
            self.assertEqual(result, value)
            self.assertEqual(type(result), type(value))

    def test_deserialize_david_to_str_no_exception(self):
        """Ensure that 'David' can be deserialized to str without raising an exception."""
        serializer = JSONUserDataSerializer()
        value = "David"
        data, metadata = serialize_value(value, serializer, "david_test")
        # This should not raise an exception
        result = deserialize_value(data, metadata)
        self.assertEqual(result, "David")
        self.assertIsInstance(result, str)

    def test_serialize_deserialize_pydantic_model_json(self):
        """Test serialization and deserialization of Pydantic models with JSON serializer."""
        serializer = JSONUserDataSerializer()
        model = TestPydanticModel(name="Alice", age=30)
        data, metadata = serialize_value(model, serializer, "model_test")
        result = deserialize_value(data, metadata)
        self.assertEqual(result, model)
        self.assertIsInstance(result, TestPydanticModel)

    def test_serialize_deserialize_pydantic_model_pickle(self):
        """Test serialization and deserialization of Pydantic models with Pickle serializer."""
        serializer = PickleUserDataSerializer()
        model = TestPydanticModel(name="Alice", age=30)
        data, metadata = serialize_value(model, serializer, "model_test")
        result = deserialize_value(data, metadata)
        self.assertEqual(result, model)
        self.assertIsInstance(result, TestPydanticModel)

    def test_json_serializer_with_multiple_possible_types(self):
        """Test JSON deserializer with multiple possible types including Pydantic models."""
        serializer = JSONUserDataSerializer()

        # Test with ModelA
        model_a = ModelA(value="test")
        data_a, metadata_a = serialize_value(model_a, serializer, "model_a")
        result_a = deserialize_value(data_a, metadata_a)
        self.assertEqual(result_a, model_a)
        self.assertIsInstance(result_a, ModelA)

        # Test with basic type
        value_str = "David"
        data_str, metadata_str = serialize_value(value_str, serializer, "str_test")
        result_str = deserialize_value(data_str, metadata_str)
        self.assertEqual(result_str, "David")
        self.assertIsInstance(result_str, str)

    def test_pickle_serializer_arbitrary_objects(self):
        """Test pickle serializer with arbitrary Python objects."""
        import datetime

        serializer = PickleUserDataSerializer()
        obj = {"complex": [1, 2, datetime.date.today()]}
        data, metadata = serialize_value(obj, serializer, "complex_test")
        result = deserialize_value(data, metadata)
        self.assertEqual(result, obj)

    def test_deserialize_value_with_file(self):
        """Test deserialization of File objects."""
        from tensorlake.applications.interface.file import File

        # For File, metadata.cls = File, content_type set
        metadata = ValueMetadata(
            id="file_test", cls=File, serializer_name=None, content_type="text/plain"
        )
        data = b"Hello World"
        result = deserialize_value(data, metadata)
        self.assertIsInstance(result, File)
        self.assertEqual(result.content, b"Hello World")
        self.assertEqual(result.content_type, "text/plain")

    def test_deserialize_value_file_missing_content_type(self):
        """Test that deserializing File without content_type raises DeserializationError."""
        from tensorlake.applications.interface.file import File

        metadata = ValueMetadata(
            id="file_test", cls=File, serializer_name=None, content_type=None
        )
        data = b"Hello"
        with self.assertRaises(DeserializationError) as cm:
            deserialize_value(data, metadata)
        self.assertIn(
            "Deserializing to File requires a content type", str(cm.exception)
        )

    def test_deserialize_value_non_file_missing_serializer(self):
        """Test that deserializing non-File without serializer_name raises DeserializationError."""
        metadata = ValueMetadata(
            id="test", cls=str, serializer_name=None, content_type=None
        )
        data = b'"test"'
        with self.assertRaises(DeserializationError) as cm:
            deserialize_value(data, metadata)
        self.assertIn("Serializer name is None for non-File value", str(cm.exception))

    def test_json_deserializer_fallback_to_json_loads(self):
        """Test that JSON deserializer falls back to json.loads for non-Pydantic types."""
        serializer = JSONUserDataSerializer()
        data, metadata = serialize_value("David", serializer, "fallback_test")
        result = deserialize_value(data, metadata)
        self.assertEqual(result, "David")
        self.assertIsInstance(result, str)

    def test_json_deserializer_with_pydantic_models_priority(self):
        """Test that Pydantic models are tried before falling back to json.loads."""
        serializer = JSONUserDataSerializer()

        # Valid model data
        model = TestModel(name="David")
        data, metadata = serialize_value(model, serializer, "model_priority")
        result = deserialize_value(data, metadata)
        self.assertEqual(result, model)
        self.assertIsInstance(result, TestModel)

    def test_json_deserializer_fails_with_unencoded_data(self):
        """Test that JSON deserializer falls back to json.loads for non-Pydantic types."""
        metadata = ValueMetadata(
            id="file_test",
            cls=type("David"),
            serializer_name="json",
            content_type="application/json",
        )
        with self.assertRaises(DeserializationError) as cm:
            _ = deserialize_value(b"David", metadata)
        self.assertEqual(
            "Failed to deserialize data with json serializer: Expecting value: line 1 column 1 (char 0)",
            str(cm.exception),
        )


if __name__ == "__main__":
    unittest.main()
