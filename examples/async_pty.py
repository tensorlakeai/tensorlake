"""Smoke test for the AsyncSandbox PTY API.

Run against the local source tree (so it picks up the AsyncPty additions
that aren't in the installed wheel yet):

    PYTHONPATH=src TENSORLAKE_API_KEY=... \\
        poetry run python examples/async_pty.py

Uses the default managed sandbox image; pass ``image=`` only if you have
a registered custom image.
"""

import asyncio

from tensorlake.sandbox import AsyncSandbox


async def main() -> None:
    async with await AsyncSandbox.create() as sbx:
        buf = bytearray()
        pty = await sbx.create_pty(
            command="/bin/bash",
            on_data=lambda data: buf.extend(data),
        )

        await pty.send_input("echo hello-from-async-pty\n")
        await pty.resize(120, 40)
        await pty.send_input("exit 7\n")

        exit_code = await pty.wait(timeout=10)
        print(f"exit_code: {exit_code}")
        print("--- captured output ---")
        print(buf.decode("utf-8", errors="replace"))


if __name__ == "__main__":
    asyncio.run(main())
