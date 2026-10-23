"""Image Service build operations through the actual Python/Rust boundary.

Requires the built native extension (``make build_cloud_sdk``). A scripted
Image Service records every request so the tests can assert on the
admission key, the cancel selector, and the absence of requests.

- A real ``KeyboardInterrupt`` during a CAS build reaches the native
  cancellation handshake (cancel by build id), is reported as an
  ``interrupted`` event, and then propagates.
- A raising callback on the CAS path stops the operation locally and
  propagates — without any remote cancellation — while the legacy emitter
  keeps swallowing callback errors and the legacy wait completes as before.
- A legacy interrupt sends no cancellation or other mutation.
"""

import http.server
import json
import os
import signal
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from tensorlake import _cloud_sdk
except ImportError:
    _cloud_sdk = None

from tensorlake.image import builds, sandbox_builder


def _build(build_id="build-1", status="pending", **extra):
    record = {
        "build_id": build_id,
        "kind": "import",
        "name": "img",
        "status": status,
        "phase": None,
        "attempt_no": 1,
        "builder_sandbox_id": None,
        "result": None,
        "operation_deadline_ms": None,
    }
    record.update(extra)
    return record


def _result(public_id="image_ok", name="img"):
    """The Image Service's trimmed tenant record (what the service sends;
    the SDK derives the template shape from it)."""
    return {
        "id": public_id,
        "name": name,
        "snapshot_id": "snap-cas",
        "global": False,
        "physical_size_bytes": 42,
        "logical_size_bytes": 10 * 1024**3,
        "created_at": "2026-10-15T00:00:00Z",
        "updated_at": "2026-10-15T00:00:00Z",
    }


class ScriptedService:
    """An HTTP server answering by (method, path) from a script and recording
    every request. Unmatched requests are answered 404 and recorded too."""

    def __init__(self):
        self.requests: list[tuple[str, str, dict, dict]] = []
        self.handlers: dict[tuple[str, str], object] = {}
        self.lock = threading.Lock()
        service = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _handle(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                try:
                    body = json.loads(raw) if raw else {}
                except json.JSONDecodeError:
                    body = {"_raw_len": len(raw)}
                headers = {key.lower(): value for key, value in self.headers.items()}
                with service.lock:
                    service.requests.append((self.command, self.path, headers, body))
                    handler = service.handlers.get((self.command, self.path))
                if handler is None:
                    status, payload = 404, {
                        "error": f"unscripted {self.command} {self.path}"
                    }
                elif callable(handler):
                    status, payload = handler(body)
                else:
                    status, payload = handler
                if status == "reset":
                    self.connection.close()
                    return
                data = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            do_GET = _handle
            do_POST = _handle
            do_PUT = _handle
            do_DELETE = _handle

            def log_message(self, *_):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def recorded(self, method, path):
        with self.lock:
            return [r for r in self.requests if r[0] == method and r[1] == path]

    def wait_for(self, method, path, timeout=20.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.recorded(method, path):
                return
            time.sleep(0.02)
        raise AssertionError(f"no {method} {path} within {timeout}s: {self.requests}")


def _env():
    return patch.dict(
        os.environ,
        {
            "TENSORLAKE_IMAGE_SERVICE_URL": "",
            "TENSORLAKE_API_KEY": "test-key",
            "TENSORLAKE_PAT": "",
            "INDEXIFY_NAMESPACE": "default",
        },
    )


def _interrupt_after(delay):
    """Deliver SIGINT to this process from a helper thread, so the default
    handler raises ``KeyboardInterrupt`` on the main thread."""
    timer = threading.Timer(delay, lambda: os.kill(os.getpid(), signal.SIGINT))
    timer.daemon = True
    timer.start()
    return timer


@unittest.skipIf(_cloud_sdk is None, "requires the built native SDK")
class TestCasInterruption(unittest.TestCase):
    def test_keyboard_interrupt_cancels_the_build_by_id_and_propagates(self):
        with ScriptedService() as service, _env():
            service.handlers[("POST", "/images/v4/builds")] = (201, _build())
            service.handlers[("GET", "/images/v4/builds/build-1")] = (
                200,
                _build(status="running"),
            )
            service.handlers[("POST", "/images/v4/builds/build-1/cancel")] = (
                200,
                {"outcome": "cancelled", "build": _build(status="cancelled")},
            )
            events = []

            def interrupt_once_polling():
                service.wait_for("GET", "/images/v4/builds/build-1")
                os.kill(os.getpid(), signal.SIGINT)

            threading.Thread(target=interrupt_once_polling, daemon=True).start()
            with self.assertRaises(KeyboardInterrupt):
                _cloud_sdk.import_sandbox_image(
                    api_url=service.url,
                    token="test-key",
                    image_reference="ubuntu:24.04",
                    registered_name="img",
                    cas=True,
                    emit=events.append,
                    docker_config_json="{}",
                )
            cancels = service.recorded("POST", "/images/v4/builds/build-1/cancel")
            self.assertEqual(len(cancels), 1, service.requests)
            # The build id rides in the path; the by-id cancel carries no body.
            self.assertEqual(cancels[0][3], {})
            admissions = service.recorded("POST", "/images/v4/builds")
            self.assertEqual(len(admissions), 1, "admission is never replayed")
            self.assertEqual(len(admissions[0][2]["idempotency-key"]), 32)
        types = [event["type"] for event in events]
        self.assertIn("build_accepted", types)
        interrupted = [event for event in events if event["type"] == "interrupted"]
        self.assertEqual(len(interrupted), 1, events)
        self.assertEqual(interrupted[0]["outcome"], "cancelled")
        self.assertEqual(interrupted[0]["build_id"], "build-1")
        self.assertTrue(interrupted[0]["confirmed"])

    def test_keyboard_interrupt_during_wait_build_cancels_and_propagates(self):
        with ScriptedService() as service, _env():
            service.handlers[("GET", "/images/v4/builds/build-1")] = (
                200,
                _build(status="running"),
            )
            service.handlers[("POST", "/images/v4/builds/build-1/cancel")] = (
                200,
                {
                    "outcome": "already_completed",
                    "build": _build(status="succeeded", result=_result()),
                },
            )
            events = []
            with patch.object(
                builds,
                "_build_context_from_env",
                return_value=type(
                    "Ctx",
                    (),
                    {
                        "api_url": service.url,
                        "api_key": "test-key",
                        "personal_access_token": None,
                        "namespace": "default",
                    },
                )(),
            ):
                handle = builds.get_build("build-1")
                _interrupt_after(0.4)
                with self.assertRaises(KeyboardInterrupt):
                    handle.wait(emit=events.append)
            self.assertEqual(
                service.recorded("POST", "/images/v4/builds/build-1/cancel")[0][3],
                {},
            )
        interrupted = [event for event in events if event["type"] == "interrupted"][0]
        self.assertEqual(interrupted["outcome"], "already_completed")
        self.assertFalse(interrupted["confirmed"])
        self.assertIn("already completed", interrupted["message"])

    def test_cas_callback_errors_propagate_without_remote_cancellation(self):
        with ScriptedService() as service, _env():
            service.handlers[("POST", "/images/v4/builds")] = (201, _build())
            service.handlers[("GET", "/images/v4/builds/build-1")] = (
                200,
                _build(status="running"),
            )
            seen = []

            def raising(event):
                seen.append(event["type"])
                if event["type"] == "build_accepted":
                    raise RuntimeError("callback boom")

            with self.assertRaises(RuntimeError) as raised:
                _cloud_sdk.import_sandbox_image(
                    api_url=service.url,
                    token="test-key",
                    image_reference="ubuntu:24.04",
                    registered_name="img",
                    cas=True,
                    emit=raising,
                    docker_config_json="{}",
                )
            self.assertEqual(str(raised.exception), "callback boom")
            time.sleep(0.3)
            self.assertEqual(
                service.recorded("POST", "/images/v4/builds/build-1/cancel"), []
            )
        self.assertEqual(seen[-1], "build_accepted", seen)

    def test_legacy_emitter_swallows_callback_errors_and_the_wait_completes(self):
        with ScriptedService() as service, _env():
            service.handlers[("POST", "/platform/v1/sandbox-template-builds")] = (
                500,
                {"message": "baseline failure"},
            )
            seen = []

            def raising(event):
                seen.append(event["type"])
                raise RuntimeError("callback boom")

            with self.assertRaises(Exception) as raised:
                _cloud_sdk.import_sandbox_image(
                    api_url=service.url,
                    token="test-key",
                    image_reference="ubuntu:24.04",
                    registered_name="img",
                    cas=False,
                    emit=raising,
                    docker_config_json="{}",
                )
            # The build ran to its baseline failure; the callback's errors
            # never surfaced and never stopped it.
            self.assertNotIsInstance(raised.exception, RuntimeError)
            self.assertIn("failed to prepare legacy image build", str(raised.exception))
            self.assertGreaterEqual(len(seen), 2, seen)
            self.assertEqual(
                len(service.recorded("POST", "/platform/v1/sandbox-template-builds")), 1
            )
            self.assertEqual(
                [r for r in service.requests if r[1].startswith("/images/v4/")],
                [],
                "legacy builds make no Image Service request",
            )

    def test_legacy_interrupt_sends_no_cancellation(self):
        """Baseline: the interrupt is delivered once the native call returns
        (possibly while its own failure is being handled) and nothing is sent
        on its behalf."""
        with ScriptedService() as service, _env():

            def slow_prepare(_body):
                time.sleep(1.5)
                return 500, {"message": "baseline failure"}

            service.handlers[("POST", "/platform/v1/sandbox-template-builds")] = (
                slow_prepare
            )
            _interrupt_after(0.5)
            outcomes = []
            try:
                try:
                    _cloud_sdk.import_sandbox_image(
                        api_url=service.url,
                        token="test-key",
                        image_reference="ubuntu:24.04",
                        registered_name="img",
                        cas=False,
                        docker_config_json="{}",
                    )
                except KeyboardInterrupt:
                    outcomes.append("interrupt")
                except Exception as exc:  # noqa: BLE001 - the baseline failure
                    outcomes.append(str(exc))
            except KeyboardInterrupt:
                outcomes.append("interrupt")
            self.assertTrue(outcomes, "the call ended")
            for outcome in outcomes:
                if outcome != "interrupt":
                    self.assertIn("failed to prepare sandbox image build", outcome)
            time.sleep(0.2)
            self.assertEqual(
                [r[1] for r in service.requests],
                ["/platform/v1/sandbox-template-builds"],
                "a legacy interrupt sends no cancel, delete or other request",
            )


@unittest.skipIf(_cloud_sdk is None, "requires the built native SDK")
class TestBuildOperationsNative(unittest.TestCase):
    def _ctx(self, url):
        return type(
            "Ctx",
            (),
            {
                "api_url": url,
                "api_key": "test-key",
                "personal_access_token": None,
                "namespace": "default",
            },
        )()

    def test_future_build_values_survive_native_decoding_as_unknown_enums(self):
        with ScriptedService() as service, _env():
            record = _build(
                status="future_status", kind="future_kind", phase="future_phase"
            )
            service.handlers[("GET", "/images/v4/builds/build-1")] = (200, record)
            service.handlers[("POST", "/images/v4/builds/build-1/cancel")] = (
                200,
                {"outcome": "future_outcome", "build": record},
            )
            with patch.object(
                builds, "_build_context_from_env", return_value=self._ctx(service.url)
            ):
                info = builds.get_build("build-1").info
                outcome = builds.cancel_build("build-1")
            self.assertIs(info.status, builds.ImageBuildStatus.UNKNOWN)
            self.assertIs(info.kind, builds.ImageBuildKind.UNKNOWN)
            self.assertIs(info.phase, builds.ImageBuildPhase.UNKNOWN)
            self.assertEqual(info.raw["status"], "future_status")
            self.assertFalse(info.is_terminal)
            self.assertIs(outcome.outcome, builds.ImageBuildCancellationOutcome.UNKNOWN)
            self.assertFalse(outcome.confirmed)
            self.assertIs(outcome.build.status, builds.ImageBuildStatus.UNKNOWN)

    def test_missing_optional_kind_from_native_is_absent(self):
        with ScriptedService() as service, _env():
            record = _build()
            record.pop("kind")
            service.handlers[("GET", "/images/v4/builds/build-1")] = (200, record)
            with patch.object(
                builds, "_build_context_from_env", return_value=self._ctx(service.url)
            ):
                info = builds.get_build("build-1").info
            self.assertIsNone(info.kind)
            self.assertEqual(info.raw["kind"], "")

    def test_upload_failures_never_expose_the_capability_url(self):
        """The context-upload capability is a signed URL. A storage error
        body that echoes it and a transport error that carries it both
        surface as an exception without any of its material."""
        secret_query = (
            "X-Amz-Algorithm=AWS4-HMAC-SHA256"
            "&X-Amz-Credential=AKIATESTKEY%2F20261015%2Fus-east-1%2Fs3%2Faws4_request"
            "&X-Amz-Signature=deadbeefcafef00d"
        )
        for failure in ("storage_error", "reset"):
            with (
                self.subTest(failure=failure),
                ScriptedService() as service,
                _env(),
                tempfile.TemporaryDirectory() as tmp,
            ):
                dockerfile = Path(tmp) / "Dockerfile"
                dockerfile.write_text("FROM scratch\n")
                upload_path = f"/upload/build-1?{secret_query}"
                admitted = _build(kind="dockerfile")
                admitted["context_upload"] = {
                    "url": service.url + upload_path,
                    "max_bytes": 10_000_000,
                }
                service.handlers[("POST", "/images/v4/builds")] = (201, admitted)
                if failure == "storage_error":
                    service.handlers[("PUT", upload_path)] = (
                        500,
                        {
                            "Code": "SignatureDoesNotMatch",
                            "StringToSign": f"AWS4-HMAC-SHA256 {service.url}{upload_path}",
                            "Resource": upload_path,
                        },
                    )
                else:
                    service.handlers[("PUT", upload_path)] = ("reset", None)
                with patch.object(
                    builds,
                    "_build_context_from_env",
                    return_value=self._ctx(service.url),
                ):
                    with self.assertRaises(builds.ImageBuildError) as raised:
                        builds.start_build(
                            str(dockerfile), registered_name="img", context_dir=tmp
                        )
                text = str(raised.exception)
                self.assertIn("context upload failed", text)
                # The object path is not a credential; everything that signs
                # it is, and none of it may survive (the signature, the access
                # key, any X-Amz-* parameter, the signing scope).
                for secret in (
                    "deadbeefcafef00d",
                    "AKIATESTKEY",
                    "X-Amz-",
                    "aws4_request",
                    secret_query,
                ):
                    self.assertNotIn(secret, text)
                self.assertEqual(len(service.recorded("PUT", upload_path)), 1)
                self.assertEqual(len(service.recorded("POST", "/images/v4/builds")), 1)

    def test_start_import_keeps_one_request_id_across_a_transient_retry(self):
        with ScriptedService() as service, _env():
            answers = iter([(503, {"error": "deploying"}), (201, _build())])
            service.handlers[("POST", "/images/v4/builds")] = lambda _body: next(
                answers
            )
            with patch.object(
                builds, "_build_context_from_env", return_value=self._ctx(service.url)
            ):
                handle = builds.start_import("ubuntu:24.04", registered_name="img")
            admissions = service.recorded("POST", "/images/v4/builds")
            self.assertEqual(len(admissions), 2)
            keys = {r[2]["idempotency-key"] for r in admissions}
            self.assertEqual(len(keys), 1)
            self.assertEqual(keys.pop(), handle.client_request_id)
            self.assertEqual(admissions[0][3]["image_ref"], "ubuntu:24.04")
        self.assertEqual(handle.build_id, "build-1")

    def test_conflict_not_found_and_not_admitted_through_the_binding(self):
        with ScriptedService() as service, _env():
            service.handlers[("POST", "/images/v4/builds")] = (
                409,
                {
                    "error": "Image 'img' already has an unfinished import 'build-3'. Cancel it before starting another.",
                    "code": "image_build_in_progress",
                    "image_name": "img",
                    "active_build_id": "build-3",
                    "active_kind": "import",
                    "active_status": "pending",
                },
            )
            service.handlers[("GET", "/images/v4/builds/build-x")] = (
                404,
                {"error": "not found"},
            )
            service.handlers[("POST", "/images/v4/builds/cancel")] = (
                200,
                {"outcome": "not_admitted", "build": None},
            )
            with patch.object(
                builds, "_build_context_from_env", return_value=self._ctx(service.url)
            ):
                with self.assertRaises(builds.ImageBuildInProgressError) as conflict:
                    builds.start_import("ubuntu:24.04", registered_name="img")
                with self.assertRaises(builds.ImageBuildNotFoundError):
                    builds.get_build("build-x")
                outcome = builds.cancel_build(client_request_id="rid-lost")
            self.assertEqual(
                service.recorded("POST", "/images/v4/builds/cancel")[0][3],
                {"client_request_id": "rid-lost"},
            )
        self.assertEqual(conflict.exception.active_build_id, "build-3")
        self.assertEqual(conflict.exception.active_status, "pending")
        self.assertEqual(outcome.outcome, "not_admitted")
        self.assertFalse(outcome.confirmed)

    def test_wait_returns_the_result_and_timeout_leaves_the_build_running(self):
        from tensorlake.image import sandbox_builder as sbm

        with ScriptedService() as service, _env():
            statuses = iter(
                [_build(status="running"), _build(status="succeeded", result=_result())]
            )
            service.handlers[("GET", "/images/v4/builds/build-1")] = lambda _body: (
                200,
                next(statuses),
            )
            service.handlers[("GET", "/images/v4/names/img")] = (
                200,
                _result(),
            )
            with (
                patch.object(
                    builds,
                    "_build_context_from_env",
                    return_value=self._ctx(service.url),
                ),
                patch.object(
                    sbm, "_build_context_from_env", return_value=self._ctx(service.url)
                ),
            ):
                result = builds.wait_build("build-1")
                looked_up = sbm.find_sandbox_image_by_name("img")
            self.assertEqual(result["id"], "image_ok")
            # One image representation through the real binding: the build
            # result and the catalog lookup are the same snake_case template
            # dict derived from the trimmed service record.
            self.assertEqual(result, looked_up)
            self.assertEqual(
                result["snapshot_format_version"], "content_addressed_streaming_v1"
            )
            self.assertEqual(result["snapshot_uri"], "snapshot://snap-cas")
            self.assertEqual(result["public"], False)
            self.assertNotIn("image_id", result)
            self.assertNotIn("build_id", result)

        with ScriptedService() as service, _env():
            service.handlers[("GET", "/images/v4/builds/build-1")] = (
                200,
                _build(status="running"),
            )
            with patch.object(
                builds, "_build_context_from_env", return_value=self._ctx(service.url)
            ):
                with self.assertRaises(builds.ImageBuildTimeoutError) as raised:
                    builds.wait_build("build-1", timeout=0.3)
            self.assertEqual(raised.exception.build_id, "build-1")
            self.assertIn("may still be running", str(raised.exception))
            self.assertEqual(
                service.recorded("POST", "/images/v4/builds/build-1/cancel"), []
            )


@unittest.skipIf(_cloud_sdk is None, "requires the built native SDK")
class TestDeleteThroughTheNativeBoundary(unittest.TestCase):
    def test_failed_legacy_cleanup_warns_in_the_cas_spelling(self):
        """``MyProject`` deletes the CAS image ``myproject``; when the legacy
        image of that name cannot be deleted, the call still succeeds and the
        warning, built from the binding's real return value, names
        ``myproject`` for recovery."""
        with (
            ScriptedService() as service,
            _env(),
            patch.dict(os.environ, {"TENSORLAKE_API_URL": service.url}),
        ):
            service.handlers[("DELETE", "/images/v4/names/myproject")] = (
                202,
                {"deleting": True},
            )
            service.handlers[
                ("GET", "/platform/v1/sandbox-templates/by-name/myproject")
            ] = (500, {"message": "internal error"})
            with self.assertWarns(UserWarning) as caught:
                sandbox_builder.delete_sandbox_image("MyProject")

        message = str(caught.warning)
        self.assertIn("Deleted CAS image 'myproject'", message)
        self.assertIn("internal error", message)
        self.assertIn("delete_sandbox_image('myproject')", message)
        self.assertNotIn("MyProject", message)
        self.assertEqual(
            [(method, path) for method, path, _, _ in service.requests],
            [
                ("DELETE", "/images/v4/names/myproject"),
                ("GET", "/platform/v1/sandbox-templates/by-name/myproject"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
