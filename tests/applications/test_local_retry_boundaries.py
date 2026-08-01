import unittest

from tensorlake.applications import (
    Retries,
    application,
    function,
    run_local_application,
)

_attempt_count = 0


@function(retries=Retries(max_retries=1))
def mutate_then_succeed(values: list[str]) -> list[str]:
    global _attempt_count
    _attempt_count += 1
    values.append(f"attempt-{_attempt_count}")
    if _attempt_count == 1:
        raise RuntimeError("retry")
    return values


@application()
@function()
def local_retry_boundary(values: list[str]) -> list[str]:
    return mutate_then_succeed(values)


class TestLocalRetryBoundaries(unittest.TestCase):
    def setUp(self) -> None:
        global _attempt_count
        _attempt_count = 0

    def test_each_attempt_receives_a_fresh_argument_copy(self) -> None:
        source = ["input"]

        output = run_local_application(local_retry_boundary, source).output()

        self.assertEqual(output, ["input", "attempt-2"])
        self.assertEqual(source, ["input"])


if __name__ == "__main__":
    unittest.main()
