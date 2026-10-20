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

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tensorlake._tracing import USER_AGENT
from tensorlake.image import Image, builds
from tensorlake.image import sandbox_builder as sbm
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


class TestResponseDecoding(unittest.TestCase):
    def test_admission_requires_a_build_identity(self):
        for payload in (
            {},
            {"build": None},
            {"build": []},
            {"build": {}},
            {"build": _build_json(build_id=None)},
            {"build": _build_json(build_id="")},
            {"build": _build_json(build_id=" ")},
            {"build": _build_json(build_id=123)},
        ):
            for native, invoke in (
                (
                    "_rust_start_image_build",
                    lambda: builds.start_build(Image(name="img", base_image="ubuntu")),
                ),
                ("_rust_start_image_import", lambda: builds.start_import("ubuntu")),
            ):
                with (
                    self.subTest(payload=payload, native=native),
                    patch.object(
                        builds, "_build_context_from_env", return_value=_ctx()
                    ),
                    patch.object(builds, native, return_value=json.dumps(payload)),
                    self.assertRaises(builds.ImageBuildError) as raised,
                ):
                    invoke()
                self.assertIs(raised.exception.code, builds.ImageBuildErrorCode.SERVICE)
                self.assertEqual(raised.exception.raw, {"response": payload})

    def test_direct_errors_preserve_dict_and_typed_info(self):
        raw = _build_json(status="future_status")
        typed = builds.ImageBuildInfo.from_dict(raw)
        for cls in (builds.ImageBuildFailedError, builds.ImageBuildCancelledError):
            with self.subTest(cls=cls):
                error = cls("m", info=raw)
                info = error.info
                self.assertIs(info.status, builds.ImageBuildStatus.UNKNOWN)
                self.assertEqual(info.raw, raw)
                self.assertIs(error.details["info"], raw)
                self.assertEqual(json.loads(json.dumps(error.details))["info"], raw)
                self.assertIs(cls("m", info=typed).info, typed)
                for malformed in ("bad", [], {"attempt_no": "bad"}):
                    self.assertIsNone(cls("m", info=malformed).info)
        error = builds.ImageBuildNotFoundError(
            "m", code=builds.ImageBuildErrorCode.NOT_FOUND, build_id="b"
        )
        self.assertIs(error.code, builds.ImageBuildErrorCode.NOT_FOUND)
        self.assertEqual(error.build_id, "b")

    def test_interrupted_error_is_strict_and_normalizes_native_results(self):
        for outcome in ("future_outcome", False):
            with self.subTest(outcome=outcome), self.assertRaises(ValueError):
                builds.ImageBuildInterruptedError("m", outcome=outcome, confirmed=True)
        error = builds.ImageBuildInterruptedError(
            "m", outcome="cancelled", confirmed=True
        )
        self.assertIs(error.outcome, builds.ImageBuildCancellationOutcome.CANCELLED)
        self.assertTrue(error.confirmed)
        raw = {
            "code": "interrupted",
            "message": "m",
            "outcome": "already_completed",
            "result": _result_json(),
        }
        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(
                builds, "_rust_wait_image_build", side_effect=_native_error(raw)
            ),
            self.assertRaises(builds.ImageBuildInterruptedError) as raised,
        ):
            builds.wait_build("b")
        error = raised.exception
        self.assertEqual(error.result["snapshot_id"], "snap-cas")
        self.assertNotIn("snapshotId", error.result)
        self.assertEqual(error.details["result"], error.result)
        self.assertEqual(error.raw, raw)

    def test_direct_error_enum_fields_are_strict(self):
        self.assertIs(
            builds.ImageBuildError("m", code="auth").code,
            builds.ImageBuildErrorCode.AUTH,
        )
        error = builds.ImageBuildInProgressError(
            "m", active_kind="import", active_status="pending"
        )
        self.assertIs(error.active_kind, builds.ImageBuildKind.IMPORT)
        self.assertIs(error.active_status, builds.ImageBuildStatus.PENDING)
        self.assertIs(error.details["active_kind"], error.active_kind)
        self.assertIs(error.details["active_status"], error.active_status)
        for cls, fields in (
            (builds.ImageBuildError, {"code": "autth"}),
            (builds.ImageBuildError, {"active_kind": "improt"}),
            (builds.ImageBuildInProgressError, {"active_kind": "improt"}),
            (builds.ImageBuildInProgressError, {"active_status": "pendign"}),
            (builds.ImageBuildInterruptedError, {"outcome": "cancelleed"}),
            (builds.ImageBuildNotFoundError, {"code": "auth"}),
            (builds.ImageBuildNotFoundError, {"code": "not_foudn"}),
        ):
            with self.subTest(cls=cls, fields=fields), self.assertRaises(ValueError):
                cls("m", **fields)

    def test_missing_or_null_build_status_fails_at_every_record_boundary(self):
        for payload in ({"build_id": "b"}, {"build_id": "b", "status": None}):
            handle = builds.ImageBuildHandle(
                builds.ImageBuildInfo.from_dict(_build_json()),
                _credentials=("http://localhost", "k", "default"),
            )
            for native, invoke, response in (
                ("_rust_get_image_build", lambda: builds.get_build("b"), payload),
                ("_rust_get_image_build", handle.refresh, payload),
                (
                    "_rust_start_image_build",
                    lambda: builds.start_build(Image(name="img", base_image="ubuntu")),
                    {"build": payload},
                ),
                (
                    "_rust_start_image_import",
                    lambda: builds.start_import("ubuntu"),
                    {"build": payload},
                ),
            ):
                with (
                    self.subTest(payload=payload, native=native),
                    patch.object(
                        builds, "_build_context_from_env", return_value=_ctx()
                    ),
                    patch.object(builds, native, return_value=json.dumps(response)),
                    self.assertRaises(builds.ImageBuildError) as raised,
                ):
                    invoke()
                self.assertIs(raised.exception.code, builds.ImageBuildErrorCode.SERVICE)
                self.assertIn("status", str(raised.exception))

    def test_build_ids_and_attempt_numbers_decode_consistently(self):
        self.assertEqual(
            builds.ImageBuildInfo.from_dict(_build_json(build_id=None)).build_id, ""
        )
        for value, expected in (
            (None, 0),
            (0, 0),
            (2, 2),
            (2.0, 2),
            ("2", 2),
            ("2.0", 2),
            (2**53 - 1, 2**53 - 1),
        ):
            with self.subTest(attempt_no=value):
                info = builds.ImageBuildInfo.from_dict(_build_json(attempt_no=value))
                self.assertEqual(info.attempt_no, expected)
                self.assertIs(type(info.attempt_no), int)
        for value in (True, False, 1.5, "1.5", 2**53, -(2**53), "bad", "", [], {}):
            with (
                self.subTest(attempt_no=value),
                self.assertRaises(builds.ImageBuildError) as raised,
            ):
                builds.ImageBuildInfo.from_dict(_build_json(attempt_no=value))
            self.assertIs(raised.exception.code, builds.ImageBuildErrorCode.SERVICE)

    def test_response_models_inherit_their_family_decoder(self):
        class CustomBuildInfo(builds.ImageBuildInfo):
            pass

        class CustomCancelOutcome(builds.CancelBuildOutcome):
            pass

        info = CustomBuildInfo.from_dict(_build_json(status="future_status"))
        self.assertIsInstance(info, CustomBuildInfo)
        self.assertIs(info.status, builds.ImageBuildStatus.UNKNOWN)
        self.assertEqual(info.attempt_no, 1)
        self.assertIs(
            CustomCancelOutcome.from_dict({"outcome": "future_outcome"}).outcome,
            builds.ImageBuildCancellationOutcome.UNKNOWN,
        )
        decoded = builds._decode_response(
            {"code": "future_code"}, builds.ImageBuildFailedError
        )
        self.assertIs(decoded["code"], builds.ImageBuildErrorCode.UNKNOWN)
        self.assertNotIn("outcome", decoded)

    def test_wire_enums_are_tolerant_and_enum_construction_is_strict(self):
        for enum in (
            SnapshotStatus,
            builds.ImageBuildStatus,
            builds.ImageBuildPhase,
            builds.ImageBuildKind,
            builds.ImageBuildErrorCode,
            builds.ImageBuildCancellationOutcome,
        ):
            for member in enum:
                self.assertIs(enum.from_response(member.value), member)
            self.assertIs(enum.from_response("future_value"), enum.UNKNOWN)
            with self.assertRaises(ValueError):
                enum("future_value")
        for field, enum in (
            ("status", builds.ImageBuildStatus),
            ("phase", builds.ImageBuildPhase),
            ("kind", builds.ImageBuildKind),
        ):
            info = builds.ImageBuildInfo.from_dict(
                _build_json(**{field: "future_value"})
            )
            self.assertIs(getattr(info, field), enum.UNKNOWN)
            self.assertEqual(info.raw[field], "future_value")
        for value in (None, ""):
            info = builds.ImageBuildInfo.from_dict(_build_json(kind=value, phase=value))
            self.assertIsNone(info.kind)
            self.assertIsNone(info.phase)

    def test_diagnostics_do_not_affect_equality_hashing_or_sdk_serialization(self):
        first = builds.ImageBuildInfo.from_dict(
            _build_json(status="succeeded", result=_result_json())
        )
        second = builds.ImageBuildInfo.from_dict(
            _build_json(
                status="succeeded", result=_result_json(), future_field="changed"
            )
        )
        self.assertEqual(first, second)
        self.assertEqual(hash(first), hash(second))
        self.assertEqual(len({first, second}), 1)
        self.assertNotIn("raw", json.loads(json.dumps(first.to_dict())))
        a = builds.CancelBuildOutcome.from_dict(
            {"outcome": "cancelled", "build": _build_json()}
        )
        b = builds.CancelBuildOutcome.from_dict(
            {"outcome": "cancelled", "build": _build_json(future_field=42)}
        )
        self.assertEqual(a, b)
        self.assertEqual(hash(a), hash(b))
        dumped = json.loads(json.dumps(a.to_dict()))
        self.assertNotIn("raw", dumped)
        self.assertNotIn("raw", dumped["build"])

    def test_errors_are_decoded_once_with_original_diagnostics(self):
        for code in builds.ImageBuildErrorCode:
            error = builds._typed_build_error(
                _native_error({"code": code.value, "message": "m"})
            )
            self.assertIs(error.code, code)
        for code in (None, "future_code"):
            raw = {
                "code": code,
                "message": "m",
                "active_kind": "future_kind",
                "active_status": "future_status",
            }
            error = builds._typed_build_error(_native_error(raw))
            self.assertEqual(error.raw, raw)
            self.assertIs(
                error.code,
                (
                    builds.ImageBuildErrorCode.INTERNAL
                    if code is None
                    else builds.ImageBuildErrorCode.UNKNOWN
                ),
            )
            self.assertIs(error.details["active_kind"], builds.ImageBuildKind.UNKNOWN)
            self.assertIs(
                error.details["active_status"], builds.ImageBuildStatus.UNKNOWN
            )
        for code in ("failed", "cancelled"):
            raw = {"code": code, "message": "m", "info": _build_json(status=None)}
            error = builds._typed_build_error(_native_error(raw))
            self.assertIsNone(error.info)
            self.assertEqual(json.loads(json.dumps(error.details))["info"], raw["info"])
            self.assertEqual(error.raw, raw)
            raw["info"]["status"] = "future_status"
            error = builds._typed_build_error(_native_error(raw))
            self.assertIs(error.info.status, builds.ImageBuildStatus.UNKNOWN)
            self.assertEqual(json.loads(json.dumps(error.details))["info"], raw["info"])
        for info in ("bad", [], {"attempt_no": "bad"}):
            error = builds._typed_build_error(
                _native_error({"code": "failed", "message": "m", "info": info})
            )
            self.assertIsInstance(error, builds.ImageBuildFailedError)
            self.assertIsNone(error.info)

    def test_unknown_cancellation_cannot_confirm(self):
        outcome = builds.CancelBuildOutcome.from_dict({"outcome": "future_outcome"})
        self.assertIs(outcome.outcome, builds.ImageBuildCancellationOutcome.UNKNOWN)
        self.assertFalse(outcome.confirmed)
        for fields in ({}, {"outcome": None}, {"outcome": "future_outcome"}):
            error = builds._typed_build_error(
                _native_error(
                    {"code": "interrupted", "message": "m", "confirmed": True, **fields}
                )
            )
            self.assertFalse(error.confirmed)
            self.assertFalse(error.details["confirmed"])
            self.assertTrue(error.raw["confirmed"])

    def test_events_remain_plain_dicts_with_the_server_spelling(self):
        raw = {
            "type": "future_event",
            "status": "future_status",
            "phase": "future_phase",
            "outcome": "future_outcome",
            "confirmed": True,
        }
        events = []

        def wait_native(*args):
            args[-1](raw)
            return json.dumps(_result_json())

        with (
            patch.object(builds, "_build_context_from_env", return_value=_ctx()),
            patch.object(builds, "_rust_wait_image_build", side_effect=wait_native),
        ):
            builds.wait_build("build-1", emit=events.append)
        self.assertEqual(events, [raw])
        self.assertIs(type(events[0]["type"]), str)
        self.assertNotIn("raw", events[0])

    def test_wait_keeps_kwargs_metadata_and_updates_diagnostics(self):
        info = builds.ImageBuildInfo(
            build_id="build-1",
            status=builds.ImageBuildStatus.RUNNING,
            kind=builds.ImageBuildKind.IMPORT,
            name="img",
            created_at_ms=123,
        )
        handle = builds.ImageBuildHandle(
            info, _credentials=("http://localhost", "k", "default")
        )
        with patch.object(
            builds, "_rust_wait_image_build", return_value=json.dumps(_result_json())
        ):
            result = handle.wait()
        self.assertEqual(handle.build_id, "build-1")
        self.assertEqual(handle.info.name, info.name)
        self.assertEqual(handle.info.kind, info.kind)
        self.assertEqual(handle.info.created_at_ms, info.created_at_ms)
        self.assertIs(handle.info.status, builds.ImageBuildStatus.SUCCEEDED)
        self.assertEqual(
            handle.info.raw, {"status": "succeeded", "result": _result_json()}
        )
        self.assertEqual(handle.info.result, result)
        hash(handle.info)

    def test_all_native_response_boundaries_keep_malformed_bodies_typed(self):
        handle = builds.ImageBuildHandle(
            builds.ImageBuildInfo.from_dict(_build_json()),
            _credentials=("http://localhost", "k", "default"),
        )
        for native, invoke in (
            ("_rust_get_image_build", lambda: builds.get_build("b")),
            ("_rust_get_image_build", handle.refresh),
            (
                "_rust_start_image_build",
                lambda: builds.start_build(Image(name="img", base_image="ubuntu")),
            ),
            ("_rust_start_image_import", lambda: builds.start_import("ubuntu")),
            ("_rust_wait_image_build", handle.wait),
            ("_rust_wait_image_build", lambda: builds.wait_build("b")),
            ("_rust_cancel_image_build", lambda: builds.cancel_build("b")),
        ):
            with (
                self.subTest(native=native, invoke=invoke),
                patch.object(builds, "_build_context_from_env", return_value=_ctx()),
                patch.object(builds, native, return_value="null"),
                self.assertRaises(builds.ImageBuildError),
            ):
                invoke()


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
            events.append,
            "{}",
        )
        self.assertEqual(handle.build_id, "build-1")
        self.assertEqual(handle.client_request_id, "rid-generated")
        self.assertEqual(handle.info.status, "pending")
        self.assertEqual(handle.info.raw["future_field"], {"ignored": True})
        self.assertIn({"type": "status", "message": "Building image 'img'..."}, events)

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
            builds._noop_emit,
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
            events.append,
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
            self.assertEqual(handle.info.raw["result"], _result_json())
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
            "m",
            build_id="b",
            error="RUN exited 1",
            info=builds.ImageBuildInfo.from_dict(_build_json(status="failed")),
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

    def test_cas_interrupted_results_keep_the_legacy_camel_case_shape(self):
        payload = {
            "code": "interrupted",
            "message": "m",
            "outcome": "already_completed",
            "result": _result_json(),
        }
        with tempfile.TemporaryDirectory() as directory:
            dockerfile = Path(directory) / "Dockerfile"
            dockerfile.write_text("FROM scratch\n")
            for native, invoke in (
                (
                    "_rust_build_sandbox_image",
                    lambda: sbm.build_sandbox_image(str(dockerfile), cas=True),
                ),
                (
                    "_rust_import_sandbox_image",
                    lambda: sbm.import_sandbox_image("ubuntu", cas=True),
                ),
            ):
                with (
                    self.subTest(native=native),
                    patch.object(sbm, "_build_context_from_env", return_value=_ctx()),
                    patch.object(sbm, native, side_effect=_native_error(payload)),
                    self.assertRaises(builds.ImageBuildInterruptedError) as raised,
                ):
                    invoke()
                self.assertEqual(raised.exception.result, _result_json())
                self.assertEqual(raised.exception.details["result"], _result_json())
                self.assertEqual(raised.exception.raw, payload)

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
