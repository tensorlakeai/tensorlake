import math
import unittest
from unittest.mock import patch

from tensorlake.applications import RETURN_WHEN, Future, SDKUsageError, function


@function()
def delayed_value(value: int) -> int:
    return value


class TestFutureValidation(unittest.TestCase):
    def test_run_later_rejects_invalid_delays_without_starting_future(self) -> None:
        for delay in (-1, math.nan, math.inf, True, "1"):
            future = delayed_value.future(1)

            with self.subTest(delay=delay):
                with self.assertRaisesRegex(
                    SDKUsageError,
                    "Future delay must be a non-negative finite number",
                ):
                    future.run_later(delay)  # type: ignore[arg-type]
                self.assertFalse(future._run_hook_was_called)
                self.assertIsNone(future._start_delay)

    def test_wait_materializes_generator_once(self) -> None:
        futures = [delayed_value.future(1), delayed_value.future(2)]
        with (
            patch("tensorlake.applications.interface.futures.runtime_hook_run_future"),
            patch(
                "tensorlake.applications.interface.futures.runtime_hook_wait_futures",
                return_value=(futures, []),
            ) as wait_hook,
        ):
            result = Future.wait(
                (future for future in futures),
                return_when=RETURN_WHEN.ALL_COMPLETED,
            )

        self.assertEqual(result, (futures, []))
        wait_hook.assert_called_once_with(
            futures=futures,
            timeout=None,
            return_when=RETURN_WHEN.ALL_COMPLETED,
        )

    def test_wait_rejects_invalid_controls_before_starting_futures(self) -> None:
        invalid_controls = [
            {"timeout": -1},
            {"timeout": math.nan},
            {"timeout": math.inf},
            {"timeout": True},
            {"return_when": "first"},
        ]
        for controls in invalid_controls:
            future = delayed_value.future(1)

            with self.subTest(controls=controls):
                with self.assertRaises(SDKUsageError):
                    Future.wait([future], **controls)  # type: ignore[arg-type]
                self.assertFalse(future._run_hook_was_called)

    def test_wait_invalid_return_when_preserves_the_sdk_error_contract(self) -> None:
        with self.assertRaises(SDKUsageError) as context:
            Future.wait([], return_when="wrong_value")  # type: ignore[arg-type]

        self.assertEqual(
            str(context.exception),
            "Not supported return_when value: 'wrong_value'",
        )


if __name__ == "__main__":
    unittest.main()
