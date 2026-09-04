#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import posixpath
import sys
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from backend.controller import GoldenPathController
from backend.github_app import GitHubError


ROOT = Path(__file__).resolve().parent


class TensorlakeCIHandler(BaseHTTPRequestHandler):
    controller: GoldenPathController
    server_version = "TensorlakeCI/0.1"
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        sys.stderr.write("[tensorlake-ci] " + format % args + "\n")

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; "
            "base-uri 'none'; object-src 'none'",
        )
        super().end_headers()

    def _json(self, status: int, payload: Any) -> None:
        encoded = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def _error(self, status: int, code: str, message: str) -> None:
        self._json(status, {"error": {"code": code, "message": message}})

    def _body(self) -> tuple[bytes, dict[str, Any]]:
        length = int(self.headers.get("Content-Length", "0"))
        if length > 1_000_000:
            raise ValueError("Request body is too large")
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return raw, {}
        try:
            return raw, json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError("Request body must be valid JSON") from exc

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/health":
                self._json(HTTPStatus.OK, {"ok": True, "mode": self.controller.mode})
            elif path == "/api/session":
                self._json(HTTPStatus.OK, self.controller.state.snapshot())
            elif path == "/api/repositories":
                self._json(HTTPStatus.OK, {"repositories": self.controller.repositories()})
            elif path == "/api/github/callback":
                query = urllib.parse.parse_qs(parsed.query)
                installation_id = int(query.get("installation_id", ["0"])[0])
                oauth_state = query.get("state", [""])[0]
                self.controller.complete_installation(installation_id, oauth_state)
                self.send_response(HTTPStatus.SEE_OTHER)
                self.send_header("Location", "/#overview")
                self.end_headers()
            elif path.startswith("/api/runs/"):
                run_id = path.removeprefix("/api/runs/").split("/", 1)[0]
                run = self.controller.state.get_run(run_id)
                if not run:
                    self._error(HTTPStatus.NOT_FOUND, "run_not_found", "Run was not found")
                else:
                    self._json(HTTPStatus.OK, run)
            elif path.startswith("/api/"):
                self._error(HTTPStatus.NOT_FOUND, "not_found", "API endpoint not found")
            else:
                self._static(path)
        except (ValueError, GitHubError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, "request_failed", str(exc))
        except Exception as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal_error", str(exc))

    def do_HEAD(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            return
        self._static(parsed.path, send_body=False)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        try:
            raw, body = self._body()
            if path == "/api/github/connect":
                self._json(HTTPStatus.OK, self.controller.connect())
            elif path == "/api/migration/plan":
                repositories = body.get("repositories")
                if not isinstance(repositories, list) or not all(
                    isinstance(item, str) for item in repositories
                ):
                    raise ValueError("repositories must be a list of full repository names")
                self._json(HTTPStatus.OK, self.controller.plan(repositories))
            elif path == "/api/migration/pull-request":
                self._json(HTTPStatus.CREATED, self.controller.create_pull_request())
            elif path == "/api/runs/smoke":
                self._json(HTTPStatus.ACCEPTED, self.controller.start_smoke_run())
            elif path == "/api/session/reset":
                self._json(HTTPStatus.OK, self.controller.reset())
            elif path.startswith("/api/runs/") and path.endswith("/retain"):
                run_id = path.removeprefix("/api/runs/").removesuffix("/retain")
                self._json(HTTPStatus.OK, self.controller.retain_run(run_id))
            elif path == "/api/github/webhook":
                signature = self.headers.get("X-Hub-Signature-256", "")
                if not self.controller.verify_webhook(raw, signature):
                    self._error(
                        HTTPStatus.UNAUTHORIZED,
                        "invalid_signature",
                        "Webhook signature verification failed",
                    )
                    return
                event = self.headers.get("X-GitHub-Event", "")
                delivery = self.headers.get("X-GitHub-Delivery", "")
                self._json(
                    HTTPStatus.ACCEPTED,
                    self.controller.handle_webhook(event, delivery, body),
                )
            else:
                self._error(HTTPStatus.NOT_FOUND, "not_found", "API endpoint not found")
        except KeyError as exc:
            self._error(HTTPStatus.NOT_FOUND, "not_found", f"Resource {exc.args[0]} was not found")
        except (ValueError, GitHubError) as exc:
            self._error(HTTPStatus.BAD_REQUEST, "request_failed", str(exc))
        except Exception as exc:
            self._error(HTTPStatus.INTERNAL_SERVER_ERROR, "internal_error", str(exc))

    def _static(self, request_path: str, send_body: bool = True) -> None:
        decoded = urllib.parse.unquote(request_path)
        normalized = posixpath.normpath(decoded).lstrip("/")
        relative = "index.html" if normalized in {"", "."} else normalized
        if relative not in {"index.html", "styles.css", "app.js"}:
            self._error(HTTPStatus.NOT_FOUND, "not_found", "File not found")
            return
        target = (ROOT / relative).resolve()
        if ROOT not in target.parents and target != ROOT:
            self._error(HTTPStatus.FORBIDDEN, "forbidden", "Path is outside the app root")
            return
        if not target.is_file() or target.name.startswith("."):
            self._error(HTTPStatus.NOT_FOUND, "not_found", "File not found")
            return
        content = target.read_bytes()
        content_type, _ = mimetypes.guess_type(target.name)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        if send_body:
            self.wfile.write(content)


def create_server(
    host: str = "127.0.0.1",
    port: int = 4173,
    mode: str = "demo",
    demo_delay: float = 0.35,
) -> ThreadingHTTPServer:
    controller = GoldenPathController(mode=mode, demo_delay=demo_delay)
    handler = type(
        "ConfiguredTensorlakeCIHandler",
        (TensorlakeCIHandler,),
        {"controller": controller},
    )
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Tensorlake CI console")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4173)
    parser.add_argument(
        "--mode", choices=("demo", "live"), default=os.getenv("TENSORLAKE_CI_MODE", "demo")
    )
    args = parser.parse_args()
    server = create_server(args.host, args.port, args.mode)
    print(f"Tensorlake CI ({args.mode}) listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
