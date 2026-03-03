import unittest

from tensorlake.applications.interface.exceptions import (
    RequestFailed,
    RequestNotFinished,
    SDKUsageError,
)
from tensorlake.applications.remote.api_client import (
    APIClient,
    RequestInput,
    _raise_as_tensorlake_error,
)


class _FakeRustClient:
    def __init__(self):
        self.run_request_called_with = None

    def close(self):
        return None

    def applications_json(self):
        return '{"applications":[{"name":"app","description":"d","tags":{},"version":"v"}]}'

    def application_manifest_json(self, application_name):
        assert application_name == "app"
        return """
{
  "name":"app",
  "description":"desc",
  "tags":{},
  "version":"v1",
  "functions":{},
  "entrypoint":{
    "function_name":"app",
    "input_serializer":"pickle",
    "inputs_base64":"gASVBgAAAAAAAABdlC4=",
    "output_serializer":"pickle",
    "output_type_hints_base64":"gAVOLg=="
  }
}
"""

    def run_request(self, application_name, inputs):
        self.run_request_called_with = (application_name, inputs)
        return "req-123"

    def wait_on_request_completion(self, application_name, request_id):
        assert application_name == "app"
        assert request_id == "req-123"

    def request_metadata_json(self, application_name, request_id):
        assert application_name == "app"
        assert request_id == "req-123"
        return '{"id":"req-123","outcome":"success","application_version":"v1","created_at":1}'

    def request_output_bytes(self, application_name, request_id):
        assert application_name == "app"
        assert request_id == "req-123"
        return (b"payload", "application/octet-stream")


class _FakeRustListBytes(_FakeRustClient):
    def request_output_bytes(self, application_name, request_id):
        assert application_name == "app"
        assert request_id == "req-123"
        return ([112, 97, 121, 108, 111, 97, 100], "application/octet-stream")


class _FakeRustNotFinished(_FakeRustClient):
    def request_metadata_json(self, application_name, request_id):
        return (
            '{"id":"req-123","outcome":null,"application_version":"v1","created_at":1}'
        )


class _FakeRustFailed(_FakeRustClient):
    def request_metadata_json(self, application_name, request_id):
        return '{"id":"req-123","outcome":{"failure":"FunctionError"},"application_version":"v1","created_at":1}'


class TestAPIClientRustBackend(unittest.TestCase):
    def test_run_request_uses_rust_backend(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        fake = _FakeRustClient()
        client._rust_client = fake

        request_id = client.run_request(
            "app",
            inputs=[
                RequestInput(
                    name="0", data=b"abc", content_type="application/octet-stream"
                )
            ],
        )

        self.assertEqual(request_id, "req-123")
        self.assertEqual(
            fake.run_request_called_with,
            ("app", [("0", b"abc", "application/octet-stream")]),
        )

    def test_application_manifest_comes_from_rust_json(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        client._rust_client = _FakeRustClient()

        manifest = client.application("app")

        self.assertEqual(manifest.name, "app")
        self.assertEqual(manifest.entrypoint.function_name, "app")
        self.assertEqual(manifest.entrypoint.inputs_base64, "gASVBgAAAAAAAABdlC4=")

    def test_request_output_not_finished_from_rust(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        client._rust_client = _FakeRustNotFinished()

        with self.assertRaises(RequestNotFinished):
            client.request_output("app", "req-123")

    def test_request_output_failed_from_rust(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        client._rust_client = _FakeRustFailed()

        with self.assertRaises(RequestFailed):
            client.request_output("app", "req-123")

    def test_request_output_success_from_rust(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        client._rust_client = _FakeRustClient()

        output = client.request_output("app", "req-123")
        self.assertEqual(output.serialized_value, b"payload")
        self.assertEqual(output.content_type, "application/octet-stream")

    def test_request_output_success_from_rust_list_bytes(self):
        client = APIClient(
            api_url="http://localhost:8900", api_key="k", namespace="default"
        )
        client._rust_client = _FakeRustListBytes()

        output = client.request_output("app", "req-123")
        self.assertEqual(output.serialized_value, b"payload")
        self.assertEqual(output.content_type, "application/octet-stream")

    def test_rust_error_auth_mapping(self):
        class FakeRustError(Exception):
            pass

        import tensorlake.applications.remote.api_client as api_client_module

        previous = api_client_module.RustCloudApiClientError
        try:
            api_client_module.RustCloudApiClientError = FakeRustError
            with self.assertRaises(SDKUsageError):
                _raise_as_tensorlake_error(
                    FakeRustError(("sdk_usage", 401, "invalid credentials"))
                )
        finally:
            api_client_module.RustCloudApiClientError = previous


if __name__ == "__main__":
    unittest.main()
