import unittest
from unittest.mock import patch

from tensorlake import cloud_client
from tensorlake.applications.interface.exceptions import InternalError


class FakeRustClientError(Exception):
    pass


class TestCloudClientErrors(unittest.TestCase):
    def test_raise_as_tensorlake_error_surfaces_connection_details(self):
        error = FakeRustClientError(
            "connection",
            None,
            "error sending request for url (http://localhost:8840/images/v3/applications): client error (Connect): tcp connect error: Connection refused (os error 61)",
        )

        with patch.object(cloud_client, "_RustClientError", FakeRustClientError):
            with self.assertRaisesRegex(
                InternalError,
                "Connection error while communicating with Tensorlake API: "
                "error sending request for url "
                r"\(http://localhost:8840/images/v3/applications\): "
                "client error \\(Connect\\): tcp connect error: Connection refused \\(os error 61\\)",
            ):
                cloud_client._raise_as_tensorlake_error(error)
