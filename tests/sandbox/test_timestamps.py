"""Timestamp parsing and serialization must agree across sandbox models."""

import unittest
from datetime import datetime, timedelta, timezone

from pydantic import TypeAdapter, ValidationError

from tensorlake.sandbox.models import (
    ListDirectoryResponse,
    OptionalTimestamp,
    OutputEvent,
    ProcessInfo,
    SandboxInfo,
    SandboxPoolInfo,
    SnapshotInfo,
    Timestamp,
)

_OPTIONAL = TypeAdapter(OptionalTimestamp)
_REQUIRED = TypeAdapter(Timestamp)
_EPOCH = 1_700_000_000.125


class TestTimestampParsing(unittest.TestCase):
    def test_numeric_epochs_keep_seconds_milliseconds_and_microseconds(self):
        fractional_inputs = (
            _EPOCH,
            _EPOCH * 1_000,
            _EPOCH * 1_000_000,
            str(_EPOCH),
            str(_EPOCH * 1_000),
            str(_EPOCH * 1_000_000),
        )
        cases = [
            (value, datetime.fromtimestamp(_EPOCH, timezone.utc))
            for value in fractional_inputs
        ] + [
            (value, datetime.fromtimestamp(float(value), timezone.utc))
            for value in (1_700_000_000, "1700000000", 0, -1)
        ]
        for value, expected in cases:
            for adapter in (_OPTIONAL, _REQUIRED):
                with self.subTest(value=value, adapter=adapter):
                    self.assertEqual(adapter.validate_python(value), expected)

    def test_iso_strings_accept_z_offsets_and_fractional_seconds(self):
        expected = datetime(2026, 10, 21, 10, 40, 0, 123456, tzinfo=timezone.utc)
        for value in (
            "2026-10-21T10:40:00.123456Z",
            "2026-10-21T10:40:00.123456+00:00",
            "2026-10-21T12:40:00.123456+02:00",
            "2026-10-21T05:10:00.123456-05:30",
        ):
            for adapter in (_OPTIONAL, _REQUIRED):
                with self.subTest(value=value, adapter=adapter):
                    self.assertEqual(adapter.validate_python(value), expected)

    def test_datetime_values_keep_their_timezone_and_identity(self):
        for value in (
            datetime(2026, 10, 21, 10, 40),
            datetime(2026, 10, 21, 10, 40, tzinfo=timezone.utc),
            datetime(2026, 10, 21, 10, 40, tzinfo=timezone(timedelta(hours=2))),
        ):
            for adapter in (_OPTIONAL, _REQUIRED):
                with self.subTest(value=value, adapter=adapter):
                    self.assertIs(adapter.validate_python(value), value)

    def test_none_is_allowed_only_for_optional_timestamps(self):
        self.assertIsNone(_OPTIONAL.validate_python(None))
        with self.assertRaises(ValidationError):
            _REQUIRED.validate_python(None)

    def test_invalid_strings_still_fail_validation(self):
        for value in ("", "tomorrow", "2026-13-01T00:00:00Z", "2026-10-21T25:00:00Z"):
            for adapter in (_OPTIONAL, _REQUIRED):
                with self.subTest(value=value, adapter=adapter):
                    with self.assertRaises(ValidationError):
                        adapter.validate_python(value)


class TestTimestampModelRoundTrips(unittest.TestCase):
    def test_json_dumps_can_be_validated_again(self):
        resources = {"cpus": 1, "memory_mb": 2048, "disk_mb": 10240}
        records = (
            SandboxInfo(
                id="sandbox",
                namespace="default",
                status="running",
                resources=resources,
                created_at=_EPOCH,
                terminated_at=_EPOCH + 1,
            ),
            SandboxPoolInfo(
                id="pool",
                namespace="default",
                image="image",
                resources=resources,
                created_at=_EPOCH,
                updated_at=_EPOCH + 1,
            ),
            SnapshotInfo(
                id="snapshot",
                namespace="default",
                sandbox_id="sandbox",
                status="completed",
                created_at=_EPOCH,
            ),
            ProcessInfo(
                pid=1,
                status="running",
                command="sleep",
                args=["60"],
                started_at=_EPOCH,
                ended_at=_EPOCH + 1,
            ),
            OutputEvent(line="hello", timestamp=_EPOCH),
            ListDirectoryResponse(
                path="/tmp",
                entries=[
                    {"name": "file", "is_dir": False, "modified_at": _EPOCH},
                    {"name": "unknown-time", "is_dir": False, "modified_at": None},
                ],
            ),
        )
        for record in records:
            with self.subTest(model=type(record).__name__):
                model = type(record)
                self.assertEqual(
                    model.model_validate(record.model_dump(mode="json", by_alias=True)),
                    record,
                )
                self.assertEqual(
                    model.model_validate_json(record.model_dump_json(by_alias=True)),
                    record,
                )


if __name__ == "__main__":
    unittest.main()
