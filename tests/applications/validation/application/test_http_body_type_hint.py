import unittest

from tensorlake.applications import HttpBody, application, function
from tensorlake.applications.validation import (
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def valid_http_body_application(body: HttpBody) -> str:
    return body.text()


@application()
@function()
def invalid_http_body_parameter(body: list[HttpBody]) -> str:
    return "invalid"


@application()
@function()
def invalid_http_body_return(body: str) -> HttpBody:
    return HttpBody(body.encode())


class TestHttpBodyTypeHint(unittest.TestCase):
    def test_rejects_nested_parameters_and_return_values(self):
        errors = [
            message
            for message in validate_loaded_applications()
            if message.severity == ValidationMessageSeverity.ERROR
        ]

        self.assertEqual(len(errors), 2)
        self.assertIn(
            "uses a HttpBody object in a complex type hint",
            errors[0].message,
        )
        self.assertEqual(
            errors[1].message,
            "Application function return type hint is an HttpBody. "
            "HttpBody is only supported for application function parameters. "
            "Use File or bytes-compatible JSON types for application return values.",
        )


if __name__ == "__main__":
    unittest.main()
