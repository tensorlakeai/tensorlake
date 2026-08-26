import json
import unittest
from unittest.mock import patch
from uuid import UUID

from pydantic import ValidationError

from tensorlake.sandbox.async_client import AsyncSandboxClient
from tensorlake.sandbox.client import SandboxClient
from tensorlake.sandbox.models import (
    ClaimSandboxRequest,
    CreateSandboxRequest,
    CreateSandboxResources,
    SandboxCredentialReference,
    SandboxCredentialVersionPolicy,
)


class _FakeRustClient:
    def __init__(self) -> None:
        self.claim_calls: list[dict[str, str]] = []

    def close(self) -> None:
        pass

    def claim_sandbox(self, **kwargs: str) -> tuple[str, str]:
        self.claim_calls.append(kwargs)
        return "trace-claim", '{"sandbox_id":"sbx-1","status":"pending"}'


class _FakeAsyncRustClient:
    def __init__(self) -> None:
        self.claim_calls: list[dict[str, str]] = []

    def close(self) -> None:
        pass

    async def claim_sandbox_async(self, **kwargs: str) -> tuple[str, str]:
        self.claim_calls.append(kwargs)
        return "trace-claim", '{"sandbox_id":"sbx-1","status":"pending"}'


def _sync_client(fake: _FakeRustClient) -> SandboxClient:
    with (
        patch("tensorlake.sandbox.client._RUST_SANDBOX_CLIENT_AVAILABLE", True),
        patch("tensorlake.sandbox.client.RustCloudSandboxClient", return_value=fake),
    ):
        return SandboxClient(
            api_url="http://localhost:8900", api_key="key", _internal=True
        )


def _async_client(fake: _FakeAsyncRustClient) -> AsyncSandboxClient:
    with (
        patch("tensorlake.sandbox.async_client._RUST_SANDBOX_CLIENT_AVAILABLE", True),
        patch(
            "tensorlake.sandbox.async_client.RustCloudSandboxClient",
            return_value=fake,
        ),
    ):
        return AsyncSandboxClient(
            api_url="http://localhost:8900", api_key="key", _internal=True
        )


class TestSandboxCredentialModels(unittest.TestCase):
    def test_serializes_client_side_name_selector_without_value_or_grant(self) -> None:
        request = CreateSandboxRequest(
            resources=CreateSandboxResources(cpus=1.0, memory_mb=1024),
            credential_references=[SandboxCredentialReference(name="github-app")],
        )

        payload = json.loads(request.model_dump_json(exclude_none=True))
        self.assertEqual(
            payload["credential_references"],
            [
                {
                    "name": "github-app",
                    "purpose": "git_https",
                    "target": "github.com",
                    "version_policy": {"policy": "active"},
                }
            ],
        )
        self.assertNotIn("value", json.dumps(payload))
        self.assertNotIn("grant", json.dumps(payload))

    def test_accepts_stable_id_and_pinned_version(self) -> None:
        reference = SandboxCredentialReference(
            secret_id=UUID("56fd06c4-7e53-45b4-99d4-4b177db5893f"),
            version_policy=SandboxCredentialVersionPolicy(
                policy="pinned",
                version_id=UUID("c36207c2-3ba9-4565-abac-e3233bca41df"),
            ),
        )
        self.assertIsNone(reference.name)

    def test_serializes_pool_claim_credential_reference(self) -> None:
        request = ClaimSandboxRequest(
            credential_references=[SandboxCredentialReference(name="github-app")]
        )
        self.assertEqual(
            json.loads(request.model_dump_json(exclude_none=True)),
            {
                "credential_references": [
                    {
                        "name": "github-app",
                        "purpose": "git_https",
                        "target": "github.com",
                        "version_policy": {"policy": "active"},
                    }
                ]
            },
        )

    def test_sync_claim_sends_credential_reference(self) -> None:
        fake = _FakeRustClient()
        client = _sync_client(fake)

        client.claim(
            "pool-1",
            credential_references=[SandboxCredentialReference(name="github-app")],
        )

        self.assertEqual(fake.claim_calls[0]["pool_id"], "pool-1")
        payload = json.loads(fake.claim_calls[0]["request_json"])
        self.assertEqual(payload["credential_references"][0]["name"], "github-app")

    def test_rejects_ambiguous_selector_and_invalid_version_policy(self) -> None:
        with self.assertRaises(ValidationError):
            SandboxCredentialReference()
        with self.assertRaises(ValidationError):
            SandboxCredentialReference(
                name="github-app",
                secret_id=UUID("56fd06c4-7e53-45b4-99d4-4b177db5893f"),
            )
        with self.assertRaises(ValidationError):
            SandboxCredentialVersionPolicy(policy="pinned")


class TestAsyncSandboxCredentialClaims(unittest.IsolatedAsyncioTestCase):
    async def test_async_claim_sends_credential_reference(self) -> None:
        fake = _FakeAsyncRustClient()
        client = _async_client(fake)

        await client.claim(
            "pool-1",
            credential_references=[SandboxCredentialReference(name="github-app")],
        )

        self.assertEqual(fake.claim_calls[0]["pool_id"], "pool-1")
        payload = json.loads(fake.claim_calls[0]["request_json"])
        self.assertEqual(payload["credential_references"][0]["name"], "github-app")


if __name__ == "__main__":
    unittest.main()
