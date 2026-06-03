#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time

from tensorlake.sandbox import SandboxClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Smoke test the Python cloud SDK create-and-connect fast path, then "
            "reconnect to the same sandbox by name with no proxy_url."
        )
    )
    parser.add_argument(
        "--image",
        default="tensorlake/ubuntu-minimal",
        help="Sandbox image to boot.",
    )
    parser.add_argument(
        "--name",
        default=f"codex-smoke-{int(time.time())}",
        help="Sandbox name to create and reconnect to.",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai"),
        help="Tensorlake API URL.",
    )
    parser.add_argument(
        "--organization-id",
        default=os.getenv("TENSORLAKE_ORGANIZATION_ID"),
        help="Optional organization scope override.",
    )
    parser.add_argument(
        "--project-id",
        default=os.getenv("TENSORLAKE_PROJECT_ID"),
        help="Optional project scope override.",
    )
    parser.add_argument(
        "--startup-timeout",
        type=float,
        default=60.0,
        help="Seconds to wait for the sandbox to become running.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Keep the sandbox instead of terminating it at the end.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.getenv("TENSORLAKE_API_KEY")
    if not api_key:
        print("TENSORLAKE_API_KEY must be set.", file=sys.stderr)
        return 2

    client = SandboxClient.for_cloud(
        api_key=api_key,
        organization_id=args.organization_id,
        project_id=args.project_id,
        api_url=args.api_url,
    )

    created = None
    reconnected = None
    try:
        print(f"Creating sandbox named {args.name!r} from image {args.image!r}...")
        created = client.create_and_connect(
            image=args.image,
            name=args.name,
            startup_timeout=args.startup_timeout,
        )

        created_info = created.info()
        created_health = created.health().value
        created_run = created.run("sh", ["-lc", "printf 'python smoke ok\\n'"])

        print(f"Created sandbox ID: {created.sandbox_id}")
        print(f"Create path ingress endpoint: {created_info.ingress_endpoint}")
        print(f"Create path healthy: {created_health.healthy}")
        print(f"Create path command stdout: {created_run.stdout.strip()}")

        created.close()
        created = None

        print(f"Reconnecting by name {args.name!r} with no proxy_url...")
        reconnected = client.connect(args.name)
        reconnect_info = reconnected.info()
        reconnect_health = reconnected.health().value
        reconnect_run = reconnected.run("sh", ["-lc", "printf 'reconnect smoke ok\\n'"])

        print(f"Reconnect resolved sandbox ID: {reconnected.sandbox_id}")
        print(f"Reconnect path ingress endpoint: {reconnect_info.ingress_endpoint}")
        print(f"Reconnect path healthy: {reconnect_health.healthy}")
        print(f"Reconnect path command stdout: {reconnect_run.stdout.strip()}")

        if not args.keep:
            print("Terminating sandbox...")
            reconnected.terminate()
            reconnected = None
        else:
            print("Keeping sandbox alive for follow-up inspection.")

        print("Python reconnect smoke test passed.")
        return 0
    finally:
        if reconnected is not None:
            if args.keep:
                reconnected.close()
            else:
                try:
                    reconnected.terminate()
                except Exception:
                    reconnected.close()
        elif created is not None:
            if args.keep:
                created.close()
            else:
                try:
                    created.terminate()
                except Exception:
                    created.close()
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
