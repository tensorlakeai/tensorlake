"""Dataplane process manager for sandbox integration tests.

Starts/stops the indexify-dataplane binary with a generated YAML config.
Adapted from the indexify repo's tests/dataplane_cli/testing.py pattern.
"""

import os
import signal
import socket
import subprocess
import sys
import tempfile
import time


def find_free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_dataplane_startup(port: int, timeout: float = 30.0) -> None:
    """Poll TCP until the dataplane proxy is listening.

    Args:
        port: TCP port to check.
        timeout: Maximum seconds to wait.

    Raises:
        TimeoutError: If the port is not listening within *timeout* seconds.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(0.5)
    raise TimeoutError(
        f"Dataplane proxy did not start on port {port} within {timeout}s"
    )


class DataplaneProcessContextManager:
    """Context manager that starts and stops the indexify-dataplane binary.

    The binary location is resolved via the ``DATAPLANE_BIN`` environment
    variable (default: ``indexify-dataplane``).

    A temporary YAML config and state file are created and cleaned up on exit.
    """

    def __init__(
        self,
        server_addr: str = "http://localhost:8901",
        driver_type: str = "docker",
    ):
        self._server_addr = server_addr
        self._driver_type = driver_type
        self._process: subprocess.Popen | None = None
        self._tmpdir: tempfile.TemporaryDirectory | None = None
        self._proxy_port: int | None = None
        self._monitoring_port: int | None = None

    @property
    def proxy_port(self) -> int:
        assert self._proxy_port is not None, "Dataplane not started"
        return self._proxy_port

    def _generate_config(self, config_path: str, state_path: str) -> None:
        self._proxy_port = find_free_port()
        self._monitoring_port = find_free_port()

        config = (
            f"env: local\n"
            f'server_addr: "{self._server_addr}"\n'
            f"driver:\n"
            f"  type: {self._driver_type}\n"
            f"http_proxy:\n"
            f"  port: {self._proxy_port}\n"
            f'  listen_addr: "0.0.0.0"\n'
            f'  advertise_address: "127.0.0.1:{self._proxy_port}"\n'
            f"monitoring:\n"
            f"  port: {self._monitoring_port}\n"
            f'state_file: "{state_path}"\n'
        )

        # Add resource overrides from env vars (for simulating CI constraints)
        cpu = os.environ.get("DATAPLANE_CPU_COUNT")
        mem = os.environ.get("DATAPLANE_MEMORY_BYTES")
        disk = os.environ.get("DATAPLANE_DISK_BYTES")
        if cpu or mem or disk:
            config += "resource_overrides:\n"
            if cpu:
                config += f"  cpu_count: {cpu}\n"
            if mem:
                config += f"  memory_bytes: {mem}\n"
            if disk:
                config += f"  disk_bytes: {disk}\n"

        with open(config_path, "w") as f:
            f.write(config)

    def start(self) -> None:
        """Start the dataplane process."""
        self._tmpdir = tempfile.TemporaryDirectory(prefix="sandbox-test-dp-")
        config_path = os.path.join(self._tmpdir.name, "config.yaml")
        state_path = os.path.join(self._tmpdir.name, "state.json")
        self._generate_config(config_path, state_path)

        binary = os.environ.get("DATAPLANE_BIN", "indexify-dataplane")
        print(f"Starting dataplane: {binary} --config {config_path}", flush=True)

        self._process = subprocess.Popen(
            [binary, "--config", config_path],
            stdout=sys.stdout,
            stderr=sys.stderr,
            preexec_fn=os.setsid,
        )

        try:
            wait_dataplane_startup(self._proxy_port)
            print(
                f"Dataplane started (pid={self._process.pid}, "
                f"proxy_port={self._proxy_port})",
                flush=True,
            )
        except TimeoutError:
            self.stop()
            raise

    def stop(self) -> None:
        """Stop the dataplane process (SIGTERM then SIGKILL)."""
        if self._process is None:
            return

        pgid = None
        try:
            pgid = os.getpgid(self._process.pid)
        except ProcessLookupError:
            pass

        if pgid is not None:
            try:
                os.killpg(pgid, signal.SIGTERM)
                self._process.wait(timeout=10)
            except (subprocess.TimeoutExpired, ProcessLookupError):
                try:
                    os.killpg(pgid, signal.SIGKILL)
                    self._process.wait(timeout=5)
                except (ProcessLookupError, subprocess.TimeoutExpired):
                    pass

        self._process = None

        if self._tmpdir is not None:
            try:
                self._tmpdir.cleanup()
            except Exception:
                pass
            self._tmpdir = None

    def __enter__(self) -> "DataplaneProcessContextManager":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
