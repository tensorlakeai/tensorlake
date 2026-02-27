"""CLI commands for sandbox management."""

from __future__ import annotations

import os
import struct
import sys
import threading
import time

import click
from rich.console import Console
from rich.table import Table

from tensorlake.cli._common import Context, require_auth_and_project
from tensorlake.sandbox import (
    ProcessStatus,
    SandboxClient,
    SandboxStatus,
)
from tensorlake.sandbox.exceptions import (
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
    SandboxNotFoundError,
)


def _handle_sandbox_error(e: SandboxError, sandbox_id: str | None = None) -> None:
    """Turn a SandboxError into a pretty, user-friendly CLI error."""
    console = Console(stderr=True)

    if isinstance(e, SandboxNotFoundError):
        console.print(f"[bold red]Sandbox not found:[/] {e.sandbox_id}")
        console.print("  Run [bold]tensorlake sbx ls[/] to see available sandboxes.")
        raise SystemExit(1)

    if isinstance(e, SandboxConnectionError):
        console.print(f"[bold red]Connection error:[/] {e}")
        console.print("  Check that the API server is reachable and your credentials are valid.")
        raise SystemExit(1)

    if isinstance(e, RemoteAPIError):
        status = e.status_code
        if status == 404 and sandbox_id:
            console.print(f"[bold red]Sandbox not found:[/] {sandbox_id}")
            console.print("  Run [bold]tensorlake sbx ls[/] to see available sandboxes.")
        elif status == 401:
            console.print("[bold red]Authentication failed.[/]")
            console.print("  Run [bold]tensorlake login[/] or check your API key.")
        elif status == 403:
            console.print("[bold red]Permission denied.[/]")
            console.print("  Check that your credentials have access to this resource.")
        elif status >= 500:
            console.print(f"[bold red]Error:[/] {e.message}")
        else:
            console.print(f"[bold red]Error ({status}):[/] {e.message}")
        raise SystemExit(1)

    # Generic fallback
    console.print(f"[bold red]Error:[/] {e}")
    raise SystemExit(1)


def _make_sandbox_client(ctx: Context) -> SandboxClient:
    """Create a SandboxClient from the CLI context."""
    return SandboxClient(
        api_url=ctx.api_url,
        api_key=ctx.api_key or ctx.personal_access_token,
        organization_id=ctx.organization_id,
        project_id=ctx.project_id,
        namespace=ctx.namespace,
    )


class SandboxGroup(click.Group):
    """Click group that falls through to 'exec' for unknown subcommands.

    Allows ``tensorlake sbx <sandbox_id> <command>`` as shorthand
    for ``tensorlake sbx exec <sandbox_id> <command>``.
    """

    def resolve_command(self, ctx, args):
        if not args:
            return super().resolve_command(ctx, args)

        # Exact match only to avoid sandbox IDs colliding with command prefixes
        cmd = click.Group.get_command(self, ctx, args[0])
        if cmd is not None:
            return cmd.name, cmd, args[1:]

        # Unknown subcommand -> redirect to exec
        exec_cmd = click.Group.get_command(self, ctx, "exec")
        if exec_cmd is not None:
            return "exec", exec_cmd, args

        return super().resolve_command(ctx, args)


@click.group("sbx", cls=SandboxGroup)
def sbx():
    """Manage and interact with sandboxes."""
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_sandbox_path(path: str) -> tuple[str | None, str]:
    """Parse ``sandbox_id:/remote/path`` or a plain local path."""
    if ":" in path:
        sandbox_id, remote_path = path.split(":", 1)
        # Avoid matching single-letter drive letters (Windows)
        if len(sandbox_id) > 1:
            return sandbox_id, remote_path
    return None, path


def _parse_env_vars(env: tuple) -> dict[str, str] | None:
    """Parse KEY=VALUE environment variable pairs."""
    if not env:
        return None
    result = {}
    for item in env:
        if "=" not in item:
            raise click.UsageError(f"Invalid env format: {item}. Use KEY=VALUE.")
        k, v = item.split("=", 1)
        result[k] = v
    return result


def _wait_and_print(sandbox, pid, timeout=None):
    """Poll process status until exit, then fetch and print stdout/stderr.

    Returns the process exit code.
    """
    deadline = time.time() + timeout if timeout else None
    info = None
    while True:
        info = sandbox.get_process(pid)
        if info.status != ProcessStatus.RUNNING:
            break
        if deadline and time.time() > deadline:
            sandbox.kill_process(pid)
            raise click.ClickException(f"Command timed out after {timeout}s")
        time.sleep(0.1)

    stdout_resp = sandbox.get_stdout(pid)
    stderr_resp = sandbox.get_stderr(pid)
    for line in stdout_resp.lines:
        sys.stdout.write(line + "\n")
    sys.stdout.flush()
    for line in stderr_resp.lines:
        sys.stderr.write(line + "\n")
    sys.stderr.flush()

    if info.exit_code is not None:
        return info.exit_code
    if info.signal is not None:
        return 128 + info.signal
    return 1


# ---------------------------------------------------------------------------
# sbx ls
# ---------------------------------------------------------------------------


@sbx.command("ls")
@require_auth_and_project
def ls_cmd(ctx: Context):
    """List all sandboxes."""
    client = _make_sandbox_client(ctx)
    try:
        sandboxes = client.list()
    except SandboxError as e:
        client.close()
        _handle_sandbox_error(e)
    finally:
        client.close()

    if not sandboxes:
        click.echo("No sandboxes found.")
        return

    _STATUS_STYLE = {
        "running": "green",
        "pending": "yellow",
        "terminated": "red",
    }

    table = Table(title="Sandboxes")
    table.add_column("ID", no_wrap=True, style="cyan")
    table.add_column("Status")
    table.add_column("Image")
    table.add_column("CPUs", justify="right")
    table.add_column("Memory", justify="right")
    table.add_column("Created At")

    for s in sandboxes:
        status_style = _STATUS_STYLE.get(s.status.value, "")
        table.add_row(
            s.sandbox_id,
            f"[{status_style}]{s.status.value}[/{status_style}]" if status_style else s.status.value,
            s.image or "-",
            str(s.resources.cpus),
            f"{s.resources.memory_mb} MB",
            s.created_at.strftime("%Y-%m-%d %H:%M:%S") if s.created_at else "-",
        )

    Console().print(table)
    count = len(sandboxes)
    click.echo(f"{count} sandbox{'es' if count != 1 else ''}")


# ---------------------------------------------------------------------------
# sbx create
# ---------------------------------------------------------------------------


@sbx.command("create")
@click.option("--image", "-i", default=None, help="Container image (server default if omitted)")
@click.option("--cpus", type=float, default=1.0, help="Number of CPUs")
@click.option("--memory", type=int, default=512, help="Memory in MB")
@click.option("--disk", type=int, default=1024, help="Ephemeral disk in MB")
@click.option("--timeout", type=int, default=None, help="Timeout in seconds")
@click.option("--entrypoint", multiple=True, help="Entrypoint command parts")
@click.option("--wait/--no-wait", default=True, help="Wait for sandbox to be running")
@require_auth_and_project
def create_cmd(ctx: Context, image, cpus, memory, disk, timeout, entrypoint, wait):
    """Create a new sandbox."""
    client = _make_sandbox_client(ctx)
    try:
        result = client.create(
            image=image,
            cpus=cpus,
            memory_mb=memory,
            ephemeral_disk_mb=disk,
            timeout_secs=timeout,
            entrypoint=list(entrypoint) if entrypoint else None,
        )
        click.echo(
            f"Created sandbox {result.sandbox_id} ({result.status.value})", err=True
        )

        if wait and result.status != SandboxStatus.RUNNING:
            click.echo("Waiting for sandbox to start...", err=True, nl=False)
            deadline = time.time() + 120
            while time.time() < deadline:
                info = client.get(result.sandbox_id)
                if info.status == SandboxStatus.RUNNING:
                    click.echo(" running", err=True)
                    click.echo(result.sandbox_id)
                    return
                if info.status == SandboxStatus.TERMINATED:
                    click.echo(" terminated", err=True)
                    raise click.ClickException("Sandbox terminated during startup")
                click.echo(".", err=True, nl=False)
                time.sleep(1)
            click.echo(" timed out", err=True)
            raise click.ClickException("Sandbox did not start within 120s")
        else:
            click.echo(result.sandbox_id)
    except SandboxError as e:
        _handle_sandbox_error(e)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# sbx cp
# ---------------------------------------------------------------------------


@sbx.command("cp")
@click.argument("src")
@click.argument("dest")
@require_auth_and_project
def cp_cmd(ctx: Context, src: str, dest: str):
    """Copy files between local machine and sandbox.

    Use sandbox_id:/path for sandbox paths.

    \b
    Examples:
        tensorlake sbx cp ./file.txt SANDBOX_ID:/remote/path
        tensorlake sbx cp SANDBOX_ID:/remote/file.txt ./local/
    """
    src_sbx, src_path = _parse_sandbox_path(src)
    dest_sbx, dest_path = _parse_sandbox_path(dest)

    if src_sbx and dest_sbx:
        raise click.UsageError(
            "Cannot copy between two sandboxes. One side must be local."
        )
    if not src_sbx and not dest_sbx:
        raise click.UsageError(
            "One of src or dest must be a sandbox path (sandbox_id:/path)."
        )

    client = _make_sandbox_client(ctx)
    try:
        if src_sbx:
            # Download: sandbox -> local
            sandbox = client.connect(src_sbx)
            try:
                data = sandbox.read_file(src_path)
                if os.path.isdir(dest_path):
                    dest_path = os.path.join(dest_path, os.path.basename(src_path))
                with open(dest_path, "wb") as f:
                    f.write(data)
                click.echo(f"{src} -> {dest_path} ({len(data)} bytes)")
            finally:
                sandbox.close()
        else:
            # Upload: local -> sandbox
            if not os.path.isfile(src_path):
                raise click.UsageError(f"Local file not found: {src_path}")
            sandbox = client.connect(dest_sbx)
            try:
                with open(src_path, "rb") as f:
                    data = f.read()
                sandbox.write_file(dest_path, data)
                click.echo(f"{src_path} -> {dest} ({len(data)} bytes)")
            finally:
                sandbox.close()
    except SandboxError as e:
        _handle_sandbox_error(e, sandbox_id=src_sbx or dest_sbx)
    finally:
        client.close()


# ---------------------------------------------------------------------------
# sbx exec  (also the fallthrough target for `sbx <sandbox_id> <cmd>`)
# ---------------------------------------------------------------------------


@sbx.command(
    "exec",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_interspersed_args=False,
    ),
)
@click.argument("sandbox_id")
@click.argument("command")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option("--timeout", "-t", type=float, default=None, help="Timeout in seconds")
@click.option("--workdir", "-w", default=None, help="Working directory")
@click.option("--env", "-e", multiple=True, help="Environment variable (KEY=VALUE)")
@require_auth_and_project
def exec_cmd(ctx: Context, sandbox_id, command, args, timeout, workdir, env):
    """Execute a command in a sandbox.

    Also invoked when you run: tensorlake sbx SANDBOX_ID COMMAND [ARGS...]

    \b
    Options must come before positional arguments:
        tensorlake sbx exec --timeout 30 SANDBOX_ID ls -la
    """
    env_dict = _parse_env_vars(env)
    exit_code = 0
    client = _make_sandbox_client(ctx)
    try:
        sandbox = client.connect(sandbox_id)
        try:
            proc = sandbox.start_process(
                command=command,
                args=list(args) if args else None,
                env=env_dict,
                working_dir=workdir,
            )
            exit_code = _wait_and_print(sandbox, proc.pid, timeout)
        finally:
            sandbox.close()
    except SandboxError as e:
        _handle_sandbox_error(e, sandbox_id=sandbox_id)
    finally:
        client.close()
    if exit_code != 0:
        sys.exit(exit_code)


# ---------------------------------------------------------------------------
# sbx run
# ---------------------------------------------------------------------------


@sbx.command(
    "run",
    context_settings=dict(
        ignore_unknown_options=True,
        allow_interspersed_args=False,
    ),
)
@click.argument("command")
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
@click.option("--image", "-i", default=None, help="Container image (server default if omitted)")
@click.option("--cpus", type=float, default=1.0, help="Number of CPUs")
@click.option("--memory", type=int, default=512, help="Memory in MB")
@click.option("--disk", type=int, default=1024, help="Ephemeral disk in MB")
@click.option(
    "--timeout", "-t", type=float, default=None, help="Command timeout in seconds"
)
@click.option("--workdir", "-w", default=None, help="Working directory")
@click.option("--env", "-e", multiple=True, help="Environment variable (KEY=VALUE)")
@click.option("--keep/--no-keep", default=False, help="Keep sandbox after command exits")
@require_auth_and_project
def run_cmd(
    ctx: Context, command, args, image, cpus, memory, disk, timeout, workdir, env, keep
):
    """Create a sandbox, run a command, and stream output.

    The sandbox is terminated when the command finishes unless --keep is used.

    \b
    Examples:
        tensorlake sbx run --image alpine:latest -- echo hello
        tensorlake sbx run -i python:3.11 -e FOO=bar -- python -c "print('hi')"
    """
    env_dict = _parse_env_vars(env)
    exit_code = 0
    client = _make_sandbox_client(ctx)
    sandbox = None
    try:
        label = f"image {image}" if image else "default image"
        click.echo(f"Creating sandbox with {label}...", err=True)
        sandbox = client.create_and_connect(
            image=image,
            cpus=cpus,
            memory_mb=memory,
            ephemeral_disk_mb=disk,
        )
        click.echo(f"Sandbox {sandbox.sandbox_id} is running.", err=True)

        proc = sandbox.start_process(
            command=command,
            args=list(args) if args else None,
            env=env_dict,
            working_dir=workdir,
        )
        exit_code = _wait_and_print(sandbox, proc.pid, timeout)

        if keep:
            click.echo(f"Sandbox {sandbox.sandbox_id} kept alive.", err=True)
            sandbox._owns_sandbox = False
            sandbox.close()
        else:
            sandbox.terminate()
        sandbox = None
    except SandboxError as e:
        if sandbox is not None:
            try:
                sandbox.terminate()
            except Exception:
                try:
                    sandbox.close()
                except Exception:
                    pass
            sandbox = None
        client.close()
        _handle_sandbox_error(e)
    finally:
        if sandbox is not None:
            try:
                sandbox.terminate()
            except Exception:
                try:
                    sandbox.close()
                except Exception:
                    pass
        client.close()
    if exit_code != 0:
        sys.exit(exit_code)


# ---------------------------------------------------------------------------
# sbx ssh
# ---------------------------------------------------------------------------


@sbx.command("ssh")
@click.argument("sandbox_id")
@click.option("--shell", "-s", default="/bin/bash", help="Shell to use")
@require_auth_and_project
def ssh_cmd(ctx: Context, sandbox_id, shell):
    """Start an interactive shell session in a sandbox.

    Opens a PTY-backed terminal with full line editing, tab completion,
    and color support.
    """
    if not sys.stdin.isatty():
        raise click.ClickException("ssh requires an interactive terminal")

    try:
        import websocket
    except ImportError:
        raise click.ClickException(
            "websocket-client is required for ssh. "
            "Install it with: pip install websocket-client"
        )

    import select
    import signal
    import termios
    import tty

    # PTY WebSocket opcodes
    OP_DATA = 0x00
    OP_RESIZE = 0x01
    OP_READY = 0x02

    client = _make_sandbox_client(ctx)
    try:
        sandbox = client.connect(sandbox_id)
        try:
            # Get terminal size
            try:
                term_size = os.get_terminal_size()
                rows, cols = term_size.lines, term_size.columns
            except OSError:
                rows, cols = 24, 80

            # Create PTY session on the sandbox
            pty_info = sandbox.create_pty_session(
                command=shell, rows=rows, cols=cols
            )
            session_id = pty_info["session_id"]
            token = pty_info["token"]

            # Connect WebSocket
            ws_url = sandbox.pty_ws_url(session_id, token)
            ws_headers = {k: v for k, v in sandbox._client.headers.items()}

            ws = websocket.WebSocket()
            ws.connect(ws_url, header=ws_headers)

            exit_code = [None]
            ws_closed = threading.Event()
            old_tty_attrs = termios.tcgetattr(sys.stdin.fileno())

            def reader():
                """Read from WebSocket and write to stdout."""
                try:
                    while True:
                        opcode, data = ws.recv_data()
                        if opcode == websocket.ABNF.OPCODE_BINARY and data:
                            if data[0] == OP_DATA:
                                os.write(sys.stdout.fileno(), data[1:])
                        elif opcode == websocket.ABNF.OPCODE_CLOSE:
                            # Close frame: 2-byte status code + reason text
                            if len(data) >= 2:
                                reason = data[2:].decode("utf-8", errors="replace")
                                if reason.startswith("exit:"):
                                    try:
                                        exit_code[0] = int(reason[5:])
                                    except ValueError:
                                        pass
                            break
                except websocket.WebSocketConnectionClosedException:
                    pass
                except Exception:
                    pass
                finally:
                    ws_closed.set()

            try:
                # Enter raw terminal mode
                tty.setraw(sys.stdin.fileno())

                # Start reader thread (WebSocket -> stdout)
                thread = threading.Thread(target=reader, daemon=True)
                thread.start()

                # Tell server we're ready to receive buffered output
                ws.send_binary(bytes([OP_READY]))

                # Handle terminal resize
                def on_sigwinch(signum, frame):
                    try:
                        sz = os.get_terminal_size()
                        ws.send_binary(
                            bytes([OP_RESIZE])
                            + struct.pack(">HH", sz.columns, sz.lines)
                        )
                    except Exception:
                        pass

                old_sigwinch = signal.getsignal(signal.SIGWINCH)
                signal.signal(signal.SIGWINCH, on_sigwinch)

                # Main loop: stdin -> WebSocket
                try:
                    while not ws_closed.is_set():
                        r, _, _ = select.select([sys.stdin], [], [], 0.1)
                        if r:
                            data = os.read(sys.stdin.fileno(), 4096)
                            if not data:
                                break
                            ws.send_binary(bytes([OP_DATA]) + data)
                except (
                    KeyboardInterrupt,
                    EOFError,
                    websocket.WebSocketConnectionClosedException,
                    BrokenPipeError,
                ):
                    pass

                ws_closed.wait(timeout=3)

            finally:
                # Restore terminal state
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_tty_attrs)
                signal.signal(signal.SIGWINCH, old_sigwinch)
                try:
                    ws.close()
                except Exception:
                    pass
        finally:
            sandbox.close()
    except SandboxError as e:
        _handle_sandbox_error(e, sandbox_id=sandbox_id)
    finally:
        client.close()

    if exit_code[0] is not None and exit_code[0] != 0:
        sys.exit(exit_code[0])
