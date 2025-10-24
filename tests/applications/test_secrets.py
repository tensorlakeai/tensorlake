import unittest

from tensorlake.applications import (
    Request,
    application,
    function,
)


@function()
def add_two(x: int) -> int:
    return x + 2


@function()
def add_three(x: int) -> int:
    return x + 3


@application()
@function(secrets=["SECRET_NAME"])
def api_router_func(x: int) -> int:
    if x % 2 == 0:
        return add_three(x)
    else:
        return add_two(x)


class TestSecrets(unittest.TestCase):
    def test_api_func_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        request: Request = api_router_func.local(2)
        self.assertEqual(request.output(), 5)


if __name__ == "__main__":
    unittest.main()
