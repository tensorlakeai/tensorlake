import unittest

from tensorlake.applications import File, HttpBody, application, function
from tensorlake.applications.function.application_call import (
    SerializedApplicationArgument,
    deserialize_application_function_call_arguments,
)
from tensorlake.applications.function.user_data_serializer import (
    deserialize_value,
    deserialize_value_with_metadata,
    serialize_value,
)
from tensorlake.applications.remote.manifests.function import (
    create_function_manifest,
)
from tensorlake.applications.user_data_serializer import PickleUserDataSerializer


class CustomFile(File):
    pass


class CustomHttpBody(HttpBody):
    pass


@application()
@function()
def raw_body_application(body: HttpBody) -> str:
    return body.text()


class TestHttpBody(unittest.TestCase):
    def test_body_accessors(self):
        body = HttpBody(b'{"event":"created"}', "application/json")

        self.assertEqual(body.content, b'{"event":"created"}')
        self.assertEqual(body.content_type, "application/json")
        self.assertEqual(body.text(), '{"event":"created"}')
        self.assertEqual(body.json(), {"event": "created"})

    def test_raw_request_body_deserialization(self):
        args, kwargs = deserialize_application_function_call_arguments(
            application=raw_body_application,
            serialized_args=[
                SerializedApplicationArgument(
                    data=memoryview(b"raw webhook payload"),
                    content_type="application/octet-stream",
                )
            ],
            serialized_kwargs={},
        )

        self.assertEqual(kwargs, {})
        self.assertEqual(len(args), 1)
        self.assertIsInstance(args[0], HttpBody)
        self.assertEqual(args[0].content, b"raw webhook payload")
        self.assertEqual(args[0].content_type, "application/octet-stream")

    def test_manifest_marks_http_body_as_raw(self):
        manifest = create_function_manifest(
            raw_body_application,
            "test-version",
            raw_body_application,
        )

        self.assertEqual(
            manifest.parameters[0].data_type.type,
            "tensorlake_http_body",
        )

    def test_raw_wrapper_subclasses_are_deserialized_as_the_annotated_type(self):
        serializer = PickleUserDataSerializer()
        for wrapper_type in (CustomFile, CustomHttpBody):
            with self.subTest(wrapper_type=wrapper_type):
                value = deserialize_value(
                    serialized_value=memoryview(b"raw payload"),
                    serializer=serializer,
                    content_type="application/octet-stream",
                    type_hint=wrapper_type,
                )

                self.assertIsInstance(value, wrapper_type)
                self.assertEqual(value.content, b"raw payload")
                self.assertEqual(value.content_type, "application/octet-stream")

    def test_file_subclass_round_trip_preserves_file_compatibility(self):
        serializer = PickleUserDataSerializer()
        serialized_value, metadata = serialize_value(
            value=CustomFile(b"file payload", "text/plain"),
            serializer=serializer,
            value_id="value-1",
            type_hint=File,
        )

        value = deserialize_value_with_metadata(serialized_value, metadata)

        self.assertIs(type(value), File)
        self.assertEqual(value.content, b"file payload")
        self.assertEqual(value.content_type, "text/plain")


if __name__ == "__main__":
    unittest.main()
