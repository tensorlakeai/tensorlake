#!/usr/bin/env python3
"""Run the current build-only v2 or v3 path against a local or dev build service."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path


def _add_vendor_nanoid_to_path() -> None:
    """Allow local runs even if top-level nanoid package is not installed."""
    repo_root = Path(__file__).resolve().parents[1]
    vendor_nanoid = repo_root / "src" / "tensorlake" / "vendor" / "nanoid"
    if vendor_nanoid.exists():
        sys.path.insert(0, str(vendor_nanoid))


_add_vendor_nanoid_to_path()

from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.code.loader import load_code
from tensorlake.cli._common import Context
from tensorlake.cli import deploy as deploy_module


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--app-file",
        default="reference_app/reference_app.py",
        help="Path to Python app file to load (default: reference_app/reference_app.py)",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("TENSORLAKE_API_URL", "https://api.tensorlake.ai"),
        help="TensorLake API URL (default: env TENSORLAKE_API_URL or cloud default)",
    )
    parser.add_argument(
        "--build-service",
        default=os.getenv("TENSORLAKE_BUILD_SERVICE"),
        help="Override build service base URL (example: http://localhost:8840/images/v3/applications)",
    )
    parser.add_argument(
        "--image-builder-version",
        choices=("v2", "v3"),
        default="v3",
        help="Select which build path to run (default: v3)",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("TENSORLAKE_API_KEY"),
        help="API key auth (default: env TENSORLAKE_API_KEY)",
    )
    parser.add_argument(
        "--pat",
        default=os.getenv("TENSORLAKE_PAT"),
        help="PAT auth (default: env TENSORLAKE_PAT)",
    )
    parser.add_argument(
        "--organization-id",
        default=os.getenv("TENSORLAKE_ORGANIZATION_ID"),
        help="Organization ID for PAT auth or explicit header injection",
    )
    parser.add_argument(
        "--project-id",
        default=os.getenv("TENSORLAKE_PROJECT_ID"),
        help="Project ID for PAT auth or explicit header injection",
    )
    return parser.parse_args()


def _resolve_org_project_from_api_key(
    api_url: str,
    api_key: str,
) -> tuple[str | None, str | None]:
    try:
        auth = Context.default(api_url=api_url, api_key=api_key)
        payload = json.loads(auth.cloud_client.introspect_api_key_json())
        return payload.get("organizationId"), payload.get("projectId")
    except Exception:
        return None, None


def _validate_auth(api_key: str | None, pat: str | None) -> None:
    if bool(api_key) == bool(pat):
        raise SystemExit(
            "Provide exactly one auth method: --api-key or --pat (or matching env vars)."
        )


async def _run_build(auth: Context, image_builder_version: str) -> None:
    functions = get_functions()
    print(f"Loaded {len(functions)} function(s)")
    builder = deploy_module.mk_builder(image_builder_version, auth)
    await deploy_module._prepare_images(builder, functions)


def main() -> int:
    args = _parse_args()
    _validate_auth(args.api_key, args.pat)

    os.environ["TENSORLAKE_API_URL"] = args.api_url
    if args.build_service:
        os.environ["TENSORLAKE_BUILD_SERVICE"] = args.build_service

    org_id = args.organization_id
    project_id = args.project_id
    if args.api_key and (not org_id or not project_id):
        resolved_org, resolved_project = _resolve_org_project_from_api_key(
            args.api_url, args.api_key
        )
        org_id = org_id or resolved_org
        project_id = project_id or resolved_project

    app_file = os.path.abspath(args.app_file)
    print(f"Loading app: {app_file}")
    load_code(app_file)

    # Match current CLI auth behavior:
    # - API key auth does not forward org/project scope headers
    # - PAT auth can carry explicit org/project scope
    auth = Context.default(
        api_url=args.api_url,
        api_key=args.api_key,
        personal_access_token=args.pat,
        organization_id=(None if args.api_key else org_id),
        project_id=(None if args.api_key else project_id),
    )
    asyncio.run(_run_build(auth, args.image_builder_version))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
