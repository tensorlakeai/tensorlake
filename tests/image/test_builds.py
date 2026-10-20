"""Image Service build operations (``tensorlake.image.builds``) without
networking: the native functions are patched and their exact arguments,
typed error decoding and the legacy boundary are asserted.

Legacy baseline: revision fe36caa5 of this branch. The legacy entry points
(``build_sandbox_image``/``import_sandbox_image`` with ``cas=False``,
lookups, listings, deletes) keep the native call shapes asserted in
``test_sandbox_builder`` and the generic error wrapping asserted here; the
CAS operations never reach them and they never reach the Image Service build
operations.
"""

import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, patch

from tensorlake._enums import ResponseEnum
from tensorlake._tracing import USER_AGENT
from tensorlake.image import Image, builds
from tensorlake.image import sandbox_builder as sbm
from tensorlake.image._build_enums import normalize_build_event
from tensorlake.sandbox.models import SnapshotStatus


def _ctx(**overrides):
    defaults = dict(
        api_url="https://api.tensorlake.test",
        api_key="tl_apiKey_abc",
        personal_access_token=None,
        organization_id="org_1",
        project_id="proj_1",
        namespace="default",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _build_json(build_id="build-1", status="pending", **extra):
    record = {
        "build_id": build_id,
        "kind": "dockerfile",
        "name": "img",
        "status": status,
        "phase": None,
        "attempt_no": 1,
        "builder_sandbox_id": None,
        "result": None,
        "operation_deadline_ms": None,
        "future_field": {"ignored": True},
    }
    record.update(extra)
    return record


def _result_json(public_id="image_ok", name="img"):
    """A build result as the Rust core hands it over: the template shape
    (camelCase keys) derived from the Image Service's trimmed record."""
    return {
        "id": public_id,
        "name": name,
        "snapshotId": "snap-cas",
        "snapshotSandboxId": None,
        "snapshotUri": "snapshot://snap-cas",
        "snapshotFormatVersion": "content_addressed_streaming_v1",
        "snapshotSizeBytes": 42,
        "rootfsDiskBytes": 10 * 1024**3,
        "rootfsNodeKind": "base",
        "public": False,
        "dockerfile": "",
        "createdAt": "2026-10-15T00:00:00Z",
        "updatedAt": "2026-10-15T00:00:00Z",
    }


def _native_error(payload: dict) -> Exception:
    """The ``CloudSandboxClientError(("image_build", None, json))`` shape the
    Rust core raises for typed build outcomes."""
    return Exception("image_build", None, json.dumps(payload))


class TestBuildEnums(unittest.TestCase):
    def test_response_enums_require_unknown_when_the_class_is_defined(self):
        with self.assertRaisesRegex(TypeError, "UNKNOWN"):

            class MissingUnknown(ResponseEnum):
                KNOWN = "known"

    def test_diagnostics_do_not_affect_equality_hashing_or_model_dumps(self):
        info = builds.ImageBuildInfo.from_dict(_build_json())
        other = builds.ImageBuildInfo.from_dict(_build_json(future_field="new value"))
        self.assertEqual(info, other)
        self.assertEqual(hash(info), hash(other))
        self.assertEqual(len({info, other}), 1)
        self.assertNotIn("raw", info.model_dump())
        self.assertNotIn("raw", json.loads(info.model_dump_json()))
        self.assertNotEqual(info, info.model_copy(update={"name": "different"}))
        first = builds.CancelBuildOutcome.from_dict(
            {"outcome": "cancelled", "build": _build_json()}
        )
        second = builds.CancelBuildOutcome.from_dict(
            {"outcome": "cancelled", "build": _build_json(), "future": 42}
        )
        self.assertEqual(first, second)
        self.assertEqual(hash(first), hash(second))
        self.assertNotIn("raw", first.model_dump())
        self.assertNotIn("raw", first.model_dump()["build"])

    def test_enum_constructors_reject_typos_and_response_decoders_accept_future_values(
        self,
    ):
        for enum in (
            SnapshotStatus,
            builds.ImageBuildStatus,
            builds.ImageBuildKind,
            builds.ImageBuildPhase,
            builds.ImageBuildCancellationOutcome,
            builds.ImageBuildErrorCode,
            builds.ImageBuildEventType,
        ):
            for value in ("future_value", None, 123):
                with self.subTest(enum=enum.__name__, value=value):
                    with self.assertRaises(ValueError):
                        enum(value)
                    self.assertIs(enum.from_response(value), enum.UNKNOWN)

    def test_model_constructors_reject_typos(self):
        with self.assertRaises(ValueError):
            builds.ImageBuildInfo(build_id="b", status="runing")
        with self.assertRaises(ValueError):
            builds.CancelBuildOutcome(outcome="canceled")
        with self.assertRaises(ValueError):
            builds.ImageBuildError("m", code="image_build_inprogress")
        with self.assertRaises(ValueError):
            builds.ImageBuildInProgressError("m", active_status="runing")

    def test_known_build_values_are_enum_members(self):
        for field, enum in (
            ("status", builds.ImageBuildStatus),
            ("kind", builds.ImageBuildKind),
            ("phase", builds.ImageBuildPhase),
        ):
            for member in enum:
                with self.subTest(field=field, value=member.value):
                    info = builds.ImageBuildInfo.from_dict(
                        _build_json(**{field: member.value})
                    )
                    self.assertIs(getattr(info, field), member)

    def test_future_build_values_are_unknown_and_raw_is_preserved(self):
        raw = _build_json(
            status="future_status", kind="future_kind", phase="future_phase"
        )
        info = builds.ImageBuildInfo.from_dict(raw)
        self.assertIs(info.status, builds.ImageBuildStatus.UNKNOWN)
        self.assertIs(info.kind, builds.ImageBuildKind.UNKNOWN)
        self.assertIs(info.phase, builds.ImageBuildPhase.UNKNOWN)
        self.assertFalse(info.is_terminal)
        self.assertEqual(info.raw, raw)
        self.assertEqual(raw["status"], "future_status")

    def test_missing_optional_build_values_remain_absent(self):
        for raw in ({"build_id": "b", "status": "pending"}, _build_json(kind=None)):
            info = builds.ImageBuildInfo.from_dict(raw)
            self.assertIsNone(info.kind)
            self.assertIsNone(info.phase)

    def test_cancellation_outcomes_are_enums_and_unknown_is_unconfirmed(self):
        for member in builds.ImageBuildCancellationOutcome:
            outcome = builds.CancelBuildOutcome.from_dict({"outcome": member.value})
            self.assertIs(outcome.outcome, member)
            self.assertEqual(
                outcome.confirmed,
                member
                in (
                    builds.ImageBuildCancellationOutcome.CANCELLED,
                    builds.ImageBuildCancellationOutcome.ALREADY_CANCELLED,
                    builds.ImageBuildCancellationOutcome.ALREADY_FAILED,
                ),
            )
        outcome = builds.CancelBuildOutcome.from_dict({"outcome": "future_outcome"})
        self.assertIs(outcome.outcome, builds.ImageBuildCancellationOutcome.UNKNOWN)
        self.assertFalse(outcome.confirmed)
        self.assertEqual(outcome.raw, {"outcome": "future_outcome"})

    def test_error_codes_are_enums_and_unknown_code_keeps_raw_details(self):
        for member in builds.ImageBuildErrorCode:
            error = builds._typed_build_error(
                _native_error({"code": member.value, "message": "build error"})
            )
            self.assertIs(error.code, member)
            self.assertIs(error.details["code"], member)
        error = builds._typed_build_error(
            _native_error({"code": "future_code", "message": "build error"})
        )
        self.assertIs(type(error), builds.ImageBuildError)
        self.assertIs(error.code, builds.ImageBuildErrorCode.UNKNOWN)
        self.assertEqual(error.raw_details["code"], "future_code")

    def test_missing_or_null_error_codes_keep_the_internal_default(self):
        for fields in ({}, {"code": None}, {"code": ""}):
            with self.subTest(fields=fields):
                payload = {"message": "m", **fields}
                error = builds._typed_build_error(_native_error(payload))
                self.assertIs(error.code, builds.ImageBuildErrorCode.INTERNAL)
                self.assertEqual(error.raw_details, payload)

    def test_nested_error_values_are_normalized(self):
        payload = {
            "code": "image_build_in_progress",
            "message": "conflict",
            "active_kind": "future_kind",
            "active_status": "future_status",
            "info": _build_json(status="future_status"),
        }
        error = builds._typed_build_error(_native_error(payload))
        self.assertIsInstance(error, builds.ImageBuildInProgressError)
        self.assertIs(error.active_kind, builds.ImageBuildKind.UNKNOWN)
        self.assertIs(error.active_status, builds.ImageBuildStatus.UNKNOWN)
        self.assertIs(error.details["info"].status, builds.ImageBuildStatus.UNKNOWN)
        self.assertEqual(error.raw_details, payload)
        absent = builds.ImageBuildInProgressError("conflict")
        self.assertIsNone(absent.active_kind)
        self.assertIsNone(absent.active_status)

    def test_unknown_interruption_cannot_confirm_cancellation(self):
        error = builds._typed_build_error(
            _native_error(
                {
                    "code": "interrupted",
                    "message": "interrupted",
                    "outcome": "future_outcome",
                    "confirmed": True,
                }
            )
        )
        self.assertIs(error.outcome, builds.ImageBuildCancellationOutcome.UNKNOWN)
        self.assertFalse(error.confirmed)
        self.assertFalse(error.details["confirmed"])
        self.assertEqual(error.raw_details["outcome"], "future_outcome")
        self.assertTrue(error.raw_details["confirmed"])
        self.assertIs(
            builds.ImageBuildInterruptedError("interrupted").outcome,
            builds.ImageBuildCancellationOutcome.UNCONFIRMED,
        )

    def test_null_status_does_not_replace_typed_failure_or_cancellation(self):
        for code, cls in (
            ("failed", builds.ImageBuildFailedError),
            ("cancelled", builds.ImageBuildCancelledError),
        ):
            with self.subTest(code=code):
                payload = {
                    "code": code,
                    "message": "m",
                    "info": _build_json(status=None),
                }
                error = builds._typed_build_error(_native_error(payload))
                self.assertIsInstance(error, cls)
                self.assertIs(error.info.status, builds.ImageBuildStatus.UNKNOWN)
                self.assertIsNone(error.info.raw["status"])
                self.assertEqual(error.raw_details, payload)

    def test_malformed_info_does_not_leak_or_replace_typed_errors(self):
        for info in ("not-a-dict", [], None, {"attempt_no": "not-a-number"}):
            with self.subTest(info=info):
                payload = {"code": "failed", "message": "m", "info": info}
                error = builds._typed_build_error(_native_error(payload))
                self.assertIsInstance(error, builds.ImageBuildFailedError)
                self.assertIsNone(error.info)
                self.assertIsNone(error.details["info"])
                self.assertEqual(error.raw_details, payload)
                self.assertIsNone(builds.ImageBuildFailedError("m", info=info).info)
        info = builds.ImageBuildInfo.from_dict(_build_json())
        self.assertIs(builds.ImageBuildFailedError("m", info=info).info, info)

    def test_confirmation_requires_an_explicit_confirmed_outcome(self):
        for fields in ({}, {"outcome": None}, {"outcome": "future_outcome"}):
            with self.subTest(fields=fields):
                error = builds._typed_build_error(
                    _native_error(
                        {
                            "code": "interrupted",
                            "message": "m",
                            "confirmed": True,
                            **fields,
                        }
                    )
                )
                self.assertFalse(error.confirmed)
                self.assertFalse(error.details["confirmed"])
                self.assertTrue(error.raw_details["confirmed"])
                event = normalize_build_event(
                    {"type": "interrupted", "confirmed": True, **fields}
                )
                self.assertFalse(event["confirmed"])
                self.assertTrue(event["raw"]["confirmed"])
        error = builds.ImageBuildInterruptedError("m", confirmed=True)
        self.assertFalse(error.confirmed)
        self.assertFalse(error.details["confirmed"])

    def test_wait_normalizes_callback_enums_without_mutating_native_events(self):
        events = []
        raw = {
            "type": "future_event",
            "status": "future_status",
            "phase": "future_phase",
            "outcome": "future_outcome",
            "confirmed": True,
            "future_field": 42,
        }

        def wait_native(*args):
            emit = args[-1]
            for member in builds.ImageBuildEventType:
                emit({"type": member.value})
            emit(raw)
            emit({"type": "build_progress", "phase": None})
            return json.dumps(_result_json())

        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(builds, "_rust_wait_image_build", side_effect=wait_native),
        ):
            builds.wait_build("build-1", emit=events.append)
        for member, event in zip(builds.ImageBuildEventType, events):
            self.assertIs(event["type"], member)
        future = events[-2]
        self.assertIs(future["type"], builds.ImageBuildEventType.UNKNOWN)
        self.assertIs(future["status"], builds.ImageBuildStatus.UNKNOWN)
        self.assertIs(future["phase"], builds.ImageBuildPhase.UNKNOWN)
        self.assertIs(future["outcome"], builds.ImageBuildCancellationOutcome.UNKNOWN)
        self.assertFalse(future["confirmed"])
        self.assertEqual(future["future_field"], 42)
        self.assertEqual(future["raw"], raw)
        self.assertNotIn("raw", future["raw"])
        self.assertEqual(raw["type"], "future_event")
        self.assertTrue(raw["confirmed"])
        self.assertIsNone(events[-1]["phase"])
        self.assertNotIn("status", events[-1])

    def test_cas_import_normalizes_callback_values(self):
        events = []

        def import_native(*args, emit, **kwargs):
            emit({"type": "build_progress", "status": "future_status"})
            return json.dumps(_result_json())

        with (
            patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
            patch.object(sbm, "_rust_import_sandbox_image", side_effect=import_native),
        ):
            sbm.import_sandbox_image("ubuntu:24.04", cas=True, emit=events.append)
        self.assertIs(events[0]["type"], builds.ImageBuildEventType.STATUS)
        self.assertIs(events[1]["status"], builds.ImageBuildStatus.UNKNOWN)
        self.assertIs(events[-1]["type"], builds.ImageBuildEventType.IMAGE_REGISTERED)

    def test_legacy_event_stream_is_unchanged_and_cas_events_are_decoded_once(self):
        for api, source, native in (
            (
                sbm.build_sandbox_image,
                Image(name="img", base_image="ubuntu:24.04"),
                "_rust_build_sandbox_image",
            ),
            (sbm.import_sandbox_image, "ubuntu:24.04", "_rust_import_sandbox_image"),
        ):
            for cas in (False, True):
                events = []
                raw = {"type": "future_event", "status": "future_status"}

                def run_native(*args, emit, **kwargs):
                    emit({"type": "build_log", "message": "line"})
                    emit(raw)
                    return json.dumps(_result_json())

                with (
                    self.subTest(api=api.__name__, cas=cas),
                    patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
                    patch.object(sbm, native, side_effect=run_native),
                ):
                    api(source, cas=cas, emit=events.append)
                    for event in events:
                        if cas:
                            self.assertIsInstance(
                                event["type"], builds.ImageBuildEventType
                            )
                            self.assertNotIn("raw", event["raw"])
                        else:
                            self.assertIs(type(event["type"]), str)
                            self.assertNotIn("raw", event)
                    self.assertEqual(events[-2]["raw"] if cas else events[-2], raw)
                    if cas:
                        self.assertIs(
                            events[-2]["type"], builds.ImageBuildEventType.UNKNOWN
                        )

    def test_verbose_output_keeps_future_event_type(self):
        output = io.StringIO()
        with patch("sys.stderr", output):
            sbm._stderr_emit(
                normalize_build_event({"type": "future_event", "message": "diagnosis"})
            )
        self.assertEqual(output.getvalue(), "[future_event] diagnosis\n")

    def test_silent_builds_do_not_decode_events(self):
        with patch.object(
            sbm,
            "normalize_build_event",
            side_effect=AssertionError("unexpected decoding"),
        ):
            emit = builds._resolve_emit(None, False)
            self.assertIs(emit, sbm._noop_emit)
            emit({"type": "build_log", "message": "line"})


class TestStartBuild(unittest.TestCase):
    def test_start_build_forwards_exact_positional_arguments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir, "Dockerfile")
            dockerfile.write_text("FROM scratch\n")
            started = json.dumps(
                {"build": _build_json(), "client_request_id": "rid-generated"}
            )
            with (
                patch.object(builds, "_build_context_from_env", return_value=_ctx()),
                patch.object(
                    builds, "_rust_start_image_build", return_value=started
                ) as native,
            ):
                events = []
                handle = builds.start_build(
                    str(dockerfile),
                    registered_name="img",
                    cpus=1.5,
                    memory_mb=2048,
                    disk_mb=20480,
                    is_public=True,
                    build_args={"A": "1"},
                    emit=events.append,
                    docker_config_json="{}",
                )
        native.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            str(dockerfile.resolve()),
            "img",
            20480,
            1.5,
            2048,
            True,
            None,
            None,
            "default",
            False,
            USER_AGENT,
            None,
            None,
            {"A": "1"},
            ANY,
            "{}",
        )
        self.assertEqual(handle.build_id, "build-1")
        self.assertEqual(handle.client_request_id, "rid-generated")
        self.assertEqual(handle.info.status, "pending")
        self.assertEqual(handle.info.raw["future_field"], {"ignored": True})
        self.assertIn(
            {
                "type": builds.ImageBuildEventType.STATUS,
                "message": "Building image 'img'...",
                "raw": {
                    "type": builds.ImageBuildEventType.STATUS,
                    "message": "Building image 'img'...",
                },
            },
            events,
        )

    def test_start_build_renders_an_image_with_an_empty_context(self):
        image = Image(name="cas-img", base_image="python:3.12-slim")
        started = json.dumps({"build": _build_json(), "client_request_id": "rid"})
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds, "_rust_start_image_build", return_value=started
            ) as native,
        ):
            builds.start_build(image)
        args = native.call_args.args
        self.assertTrue(args[2].endswith("/Dockerfile"))
        self.assertEqual(args[3], "cas-img")
        self.assertIn("FROM python:3.12-slim", args[13])
        self.assertTrue(args[14])  # a temporary, empty context directory

    def test_start_import_forwards_exact_positional_arguments(self):
        started = json.dumps({"build": _build_json(), "client_request_id": "rid"})
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds, "_rust_start_image_import", return_value=started
            ) as native,
        ):
            handle = builds.start_import("ghcr.io/org/app:v1", docker_config_json="{}")
        native.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "ghcr.io/org/app:v1",
            "app",
            None,
            2.0,
            4096,
            False,
            None,
            None,
            "default",
            False,
            USER_AGENT,
            ANY,
            "{}",
        )
        self.assertEqual(handle.build_id, "build-1")

    def test_requires_an_api_key(self):
        with patch.object(
            builds,
            "_build_context_from_env",
            return_value=_ctx(api_key=None, personal_access_token="pat"),
        ):
            with self.assertRaises(builds.ImageBuildError) as raised:
                builds.start_import("ubuntu:24.04")
        self.assertEqual(raised.exception.code, "auth")
        self.assertIn("CLI-only", str(raised.exception))

    def test_conflict_is_typed_with_the_active_build(self):
        conflict = _native_error(
            {
                "code": "image_build_in_progress",
                "message": "Image 'img' already has an unfinished build 'build-9'. Cancel it before starting another.",
                "build_id": "build-9",
                "image_name": "img",
                "active_build_id": "build-9",
                "active_kind": "dockerfile",
                "active_status": "running",
            }
        )
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(builds, "_rust_start_image_import", side_effect=conflict),
        ):
            with self.assertRaises(builds.ImageBuildInProgressError) as raised:
                builds.start_import("ubuntu:24.04", registered_name="img")
        error = raised.exception
        self.assertEqual(error.active_build_id, "build-9")
        self.assertEqual(error.active_kind, "dockerfile")
        self.assertEqual(error.active_status, "running")
        self.assertEqual(error.image_name, "img")
        self.assertIn("Cancel it before starting another", str(error))
        self.assertIsInstance(error, sbm.SandboxImageBuildError)


class TestGetWaitCancel(unittest.TestCase):
    def test_malformed_native_responses_remain_build_errors(self):
        info = builds.ImageBuildInfo.from_dict(_build_json())
        handle = builds.ImageBuildHandle(
            info, _credentials=("https://api.tensorlake.test", "k", "default")
        )
        malformed = _build_json(name={"future": "shape"})
        for native, invoke, response in (
            ("_rust_get_image_build", lambda: builds.get_build("build-1"), malformed),
            ("_rust_get_image_build", handle.refresh, malformed),
            (
                "_rust_start_image_build",
                lambda: builds.start_build(Image(name="img", base_image="ubuntu")),
                {"build": malformed},
            ),
            (
                "_rust_start_image_import",
                lambda: builds.start_import("ubuntu"),
                {"build": malformed},
            ),
            ("_rust_wait_image_build", handle.wait, []),
            ("_rust_wait_image_build", lambda: builds.wait_build("build-1"), []),
            ("_rust_cancel_image_build", lambda: builds.cancel_build("build-1"), []),
        ):
            body = json.dumps(response)
            with (
                self.subTest(operation=invoke, native=native),
                patch.object(builds, "_build_context_from_env", return_value=_ctx()),
                patch.object(builds, native, return_value=body),
                self.assertRaises(builds.ImageBuildError) as raised,
            ):
                invoke()
            self.assertIs(raised.exception.code, builds.ImageBuildErrorCode.SERVICE)
            self.assertEqual(raised.exception.raw_details["response"], body)
            self.assertIs(handle.info, info)

    def test_wait_preserves_kwargs_constructed_handle_metadata(self):
        for raw in ({}, {"name": {"future": "shape"}}):
            with self.subTest(raw=raw):
                info = builds.ImageBuildInfo(
                    build_id="build-1",
                    status=builds.ImageBuildStatus.RUNNING,
                    kind=builds.ImageBuildKind.DOCKERFILE,
                    name="img",
                    attempt_no=3,
                    created_at_ms=123,
                    updated_at_ms=456,
                    raw=raw,
                )
                handle = builds.ImageBuildHandle(
                    info, _credentials=("https://api.tensorlake.test", "k", "default")
                )
                with patch.object(
                    builds,
                    "_rust_wait_image_build",
                    return_value=json.dumps(_result_json()),
                ):
                    result = handle.wait()
                self.assertEqual(handle.build_id, "build-1")
                self.assertEqual(
                    handle.info.model_dump(exclude={"status", "result"}),
                    info.model_dump(exclude={"status", "result"}),
                )
                self.assertIs(handle.info.status, builds.ImageBuildStatus.SUCCEEDED)
                self.assertEqual(handle.info.result, result)
                self.assertEqual(info.raw, raw)

    def test_get_build_returns_a_handle_and_refreshes(self):
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_get_image_build",
                side_effect=[
                    json.dumps(_build_json(status="running")),
                    json.dumps(_build_json(status="succeeded", result=_result_json())),
                ],
            ) as native,
        ):
            handle = builds.get_build("build-1")
            self.assertEqual(handle.info.status, "running")
            self.assertFalse(handle.info.is_terminal)
            info = handle.refresh()
        self.assertEqual(info.status, "succeeded")
        self.assertTrue(info.is_terminal)
        self.assertEqual(info.result["id"], "image_ok")
        native.assert_called_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "build-1",
            None,
            None,
            "default",
            False,
            USER_AGENT,
        )

    def test_legacy_and_image_references_are_refused_before_any_call(self):
        for reference in ("sandbox_template_1", "image_abc123", " "):
            with (
                patch.object(builds, "_build_context_from_env", return_value=_ctx()),
                patch.object(builds, "_rust_get_image_build") as get_native,
                patch.object(builds, "_rust_cancel_image_build") as cancel_native,
                patch.object(builds, "_rust_wait_image_build") as wait_native,
            ):
                with self.assertRaises(builds.ImageBuildError) as raised:
                    builds.get_build(reference)
                self.assertEqual(raised.exception.code, "usage")
                with self.assertRaises(builds.ImageBuildError):
                    builds.cancel_build(reference)
                with self.assertRaises(builds.ImageBuildError):
                    builds.wait_build(reference)
            get_native.assert_not_called()
            cancel_native.assert_not_called()
            wait_native.assert_not_called()

    def test_not_found_is_typed(self):
        missing = _native_error(
            {
                "code": "not_found",
                "message": "build build-x not found",
                "build_id": "build-x",
            }
        )
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(builds, "_rust_get_image_build", side_effect=missing),
        ):
            with self.assertRaises(builds.ImageBuildNotFoundError) as raised:
                builds.get_build("build-x")
        self.assertEqual(raised.exception.build_id, "build-x")

    def test_wait_forwards_options_and_returns_the_result(self):
        events = []
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_get_image_build",
                return_value=json.dumps(_build_json(status="running")),
            ),
            patch.object(
                builds,
                "_rust_wait_image_build",
                return_value=json.dumps(_result_json()),
            ) as native,
        ):
            handle = builds.get_build("build-1")
            result = handle.wait(timeout=12.5, follow_logs=True, emit=events.append)
        native.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "build-1",
            12.5,
            True,
            None,
            None,
            "default",
            False,
            USER_AGENT,
            ANY,
        )
        self.assertEqual(result["id"], "image_ok")
        self.assertEqual(
            result["snapshot_format_version"], "content_addressed_streaming_v1"
        )
        self.assertEqual(handle.info.status, "succeeded")
        self.assertEqual(handle.info.result["id"], "image_ok")

    def test_results_are_the_catalog_lookup_dict(self):
        """``wait()``, a refreshed ``info.result`` and
        ``find_sandbox_image_by_name`` present one image representation:
        snake_case keys over the template shape, with no digest or build
        provenance extras."""
        from tensorlake.image import sandbox_builder as sbm

        core_result = json.dumps(_result_json())
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_get_image_build",
                return_value=json.dumps(
                    _build_json(status="succeeded", result=_result_json())
                ),
            ),
            patch.object(builds, "_rust_wait_image_build", return_value=core_result),
            patch.object(
                sbm, "_rust_find_sandbox_image_by_name", return_value=core_result
            ),
        ):
            handle = builds.get_build("build-1")
            looked_up = sbm.find_sandbox_image_by_name("img")
            self.assertEqual(handle.info.result, looked_up)
            self.assertEqual(handle.wait(), looked_up)
            self.assertEqual(handle.info.raw["result"], looked_up)
        self.assertEqual(looked_up["snapshot_id"], "snap-cas")
        self.assertEqual(looked_up["snapshot_uri"], "snapshot://snap-cas")
        self.assertEqual(looked_up["rootfs_disk_bytes"], 10 * 1024**3)
        self.assertNotIn("snapshotId", looked_up)
        self.assertNotIn("image_id", looked_up)
        self.assertNotIn("build_id", looked_up)
        # The trimmed build view carries no cleanup state or upload capability.
        info = builds.ImageBuildInfo.from_dict(
            _build_json(
                cleanup_pending=True,
                context_upload={"url": "https://upload.test/ctx", "max_bytes": 1},
            )
        )
        self.assertFalse(hasattr(info, "cleanup_pending"))
        self.assertFalse(hasattr(info, "context_upload"))

    def test_wait_distinguishes_failed_cancelled_and_timeout(self):
        cases = [
            (
                {
                    "code": "failed",
                    "message": "build build-1 failed: RUN exited 1",
                    "build_id": "build-1",
                    "error": "RUN exited 1",
                    "info": _build_json(status="failed", error="RUN exited 1"),
                },
                builds.ImageBuildFailedError,
            ),
            (
                {
                    "code": "cancelled",
                    "message": "build build-1 was cancelled",
                    "build_id": "build-1",
                    "info": _build_json(status="cancelled"),
                },
                builds.ImageBuildCancelledError,
            ),
            (
                {
                    "code": "timeout",
                    "message": "build build-1 did not finish within 7s of waiting; it may still be running",
                    "build_id": "build-1",
                    "waited_secs": 7,
                },
                builds.ImageBuildTimeoutError,
            ),
        ]
        for payload, expected in cases:
            with self.subTest(code=payload["code"]):
                with (
                    patch.object(
                        builds, "_build_context_from_env", return_value=_ctx()
                    ),
                    patch.object(
                        builds,
                        "_rust_get_image_build",
                        return_value=json.dumps(_build_json(status="running")),
                    ),
                    patch.object(
                        builds,
                        "_rust_wait_image_build",
                        side_effect=_native_error(payload),
                    ),
                ):
                    with self.assertRaises(expected) as raised:
                        builds.wait_build("build-1")
                self.assertEqual(raised.exception.build_id, "build-1")
                self.assertEqual(raised.exception.code, payload["code"])
        failed = builds.ImageBuildFailedError(
            "m", build_id="b", error="RUN exited 1", info=_build_json(status="failed")
        )
        self.assertEqual(failed.error, "RUN exited 1")
        self.assertEqual(failed.info.status, "failed")

    def test_wait_build_runs_the_lookup_inside_the_interruptible_wait(self):
        """``wait_build`` makes no separate lookup call before the native
        wait: the lookup is part of the interruptible operation, so an
        interrupt during it still cancels by build id and ``timeout`` bounds
        it too."""
        core_result = json.dumps({"id": "image_1", "snapshotId": "snap-1"})
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(builds, "_rust_get_image_build") as get_native,
            patch.object(
                builds, "_rust_wait_image_build", return_value=core_result
            ) as wait_native,
        ):
            result = builds.wait_build("build-1", timeout=12.5, follow_logs=True)
        self.assertEqual(result, {"id": "image_1", "snapshot_id": "snap-1"})
        get_native.assert_not_called()
        wait_native.assert_called_once()
        args = wait_native.call_args.args
        self.assertEqual(args[2], "build-1")
        self.assertEqual(args[3], 12.5)
        self.assertTrue(args[4])

    def test_cancel_by_build_id_alone_and_outcomes(self):
        responses = [
            {"outcome": "cancelled", "build": _build_json(status="cancelled")},
            {
                "outcome": "already_completed",
                "build": _build_json(status="succeeded", result=_result_json()),
            },
            {"outcome": "not_admitted", "build": None},
        ]
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_cancel_image_build",
                side_effect=[json.dumps(response) for response in responses],
            ) as native,
        ):
            cancelled = builds.cancel_build("build-1")
            completed = builds.cancel_build("build-1")
            not_admitted = builds.cancel_build(client_request_id="rid-lost")
        # Cancel by build id needs nothing else.
        self.assertEqual(
            native.call_args_list[0].args,
            (
                "https://api.tensorlake.test",
                "tl_apiKey_abc",
                "build-1",
                None,
                None,
                None,
                "default",
                False,
                USER_AGENT,
            ),
        )
        self.assertEqual(native.call_args_list[2].args[2:4], (None, "rid-lost"))
        self.assertEqual(cancelled.outcome, "cancelled")
        self.assertTrue(cancelled.confirmed)
        self.assertEqual(cancelled.build.status, "cancelled")
        self.assertEqual(completed.outcome, "already_completed")
        self.assertFalse(completed.confirmed)
        self.assertEqual(completed.build.result["id"], "image_ok")
        self.assertEqual(not_admitted.outcome, "not_admitted")
        self.assertFalse(not_admitted.confirmed)
        self.assertIsNone(not_admitted.build)

    def test_cancel_requires_exactly_one_selector(self):
        with patch.object(builds, "_rust_cancel_image_build") as native:
            with self.assertRaises(builds.ImageBuildError):
                builds.cancel_build()
            with self.assertRaises(builds.ImageBuildError):
                builds.cancel_build("build-1", client_request_id="rid")
        native.assert_not_called()

    def test_handle_cancel_uses_the_build_id(self):
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_get_image_build",
                return_value=json.dumps(_build_json(status="running")),
            ),
            patch.object(
                builds,
                "_rust_cancel_image_build",
                return_value=json.dumps({"outcome": "cancelled", "build": None}),
            ) as native,
        ):
            outcome = builds.get_build("build-1").cancel()
        self.assertEqual(outcome.outcome, "cancelled")
        self.assertEqual(native.call_args.args[2], "build-1")
        self.assertIsNone(native.call_args.args[3])

    def test_interrupted_is_typed(self):
        payload = {
            "code": "interrupted",
            "message": "Build build-1 already completed",
            "build_id": "build-1",
            "client_request_id": "rid",
            "outcome": "already_completed",
            "confirmed": False,
            "result": _result_json(),
        }
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds,
                "_rust_get_image_build",
                return_value=json.dumps(_build_json(status="running")),
            ),
            patch.object(
                builds, "_rust_wait_image_build", side_effect=_native_error(payload)
            ),
        ):
            with self.assertRaises(builds.ImageBuildInterruptedError) as raised:
                builds.wait_build("build-1")
        self.assertEqual(raised.exception.outcome, "already_completed")
        self.assertFalse(raised.exception.confirmed)
        self.assertEqual(raised.exception.result["id"], "image_ok")


class TestLegacyBoundary(unittest.TestCase):
    """The legacy convenience functions keep their baseline behavior: generic
    error wrapping, ``cas=False`` forwarding, and no Image Service build
    operation; ``cas=True`` surfaces the typed outcomes."""

    def test_legacy_build_keeps_generic_error_wrapping(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile = Path(tmpdir, "Dockerfile")
            dockerfile.write_text("FROM scratch\n")
            with (
                patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
                patch.object(
                    sbm,
                    "_rust_build_sandbox_image",
                    side_effect=Exception(
                        "sandbox_image_build", None, "rootfs build preparation failed"
                    ),
                ) as native,
                patch.object(builds, "_rust_start_image_build") as start_native,
            ):
                with self.assertRaises(sbm.SandboxImageBuildError) as raised:
                    sbm.build_sandbox_image(str(dockerfile), registered_name="img")
        self.assertIs(type(raised.exception), sbm.SandboxImageBuildError)
        self.assertEqual(
            str(raised.exception),
            "Exception: ('sandbox_image_build', None, 'rootfs build preparation failed')",
        )
        self.assertIs(native.call_args.kwargs["cas"], False)
        start_native.assert_not_called()

    def test_legacy_import_keeps_generic_error_wrapping(self):
        with (
            patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                sbm,
                "_rust_import_sandbox_image",
                side_effect=Exception("sandbox_image_import", None, "boom"),
            ) as native,
        ):
            with self.assertRaises(sbm.SandboxImageBuildError) as raised:
                sbm.import_sandbox_image("ubuntu:24.04")
        self.assertIs(type(raised.exception), sbm.SandboxImageBuildError)
        self.assertIs(native.call_args.kwargs["cas"], False)

    def test_cas_build_surfaces_typed_outcomes(self):
        conflict = _native_error(
            {
                "code": "image_build_in_progress",
                "message": "Image 'img' already has an unfinished import 'build-2'. Cancel it before starting another.",
                "build_id": "build-2",
                "image_name": "img",
                "active_build_id": "build-2",
                "active_kind": "import",
                "active_status": "pending",
            }
        )
        with (
            patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
            patch.object(sbm, "_rust_import_sandbox_image", side_effect=conflict),
        ):
            with self.assertRaises(builds.ImageBuildInProgressError) as raised:
                sbm.import_sandbox_image(
                    "ubuntu:24.04", registered_name="img", cas=True
                )
        self.assertEqual(raised.exception.active_build_id, "build-2")
        self.assertEqual(raised.exception.active_status, "pending")

    def test_cas_build_without_a_typed_payload_keeps_the_generic_wrapping(self):
        with (
            patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                sbm,
                "_rust_import_sandbox_image",
                side_effect=Exception(
                    "sandbox_image_import", None, "context upload failed"
                ),
            ),
        ):
            with self.assertRaises(sbm.SandboxImageBuildError) as raised:
                sbm.import_sandbox_image("ubuntu:24.04", cas=True)
        self.assertIs(type(raised.exception), sbm.SandboxImageBuildError)


if __name__ == "__main__":
    unittest.main()
