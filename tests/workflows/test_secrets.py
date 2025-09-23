import unittest

import tensorlake.workflows.interface as tensorlake


@tensorlake.function()
def add_two(x: int) -> int:
    return x + 2


@tensorlake.function()
def add_three(x: int) -> int:
    return x + 3


@tensorlake.api()
@tensorlake.function(secrets=["SECRET_NAME"])
def api_router_func(x: int) -> int:
    if x % 2 == 0:
        return add_three(x)
    else:
        return add_two(x)


class TestSecrets(unittest.TestCase):
    def test_api_func_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        request: tensorlake.Request = tensorlake.call_local_api(
            api_router_func,
            2,
        )
        self.assertEqual(request.output(), 5)


if __name__ == "__main__":
    unittest.main()
