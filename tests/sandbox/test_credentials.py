import json
import unittest
from uuid import UUID

from pydantic import ValidationError

from tensorlake.sandbox.models import (
    CreateSandboxRequest,
    CreateSandboxResources,
    SandboxCredentialReference,
    SandboxCredentialVersionPolicy,
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


if __name__ == "__main__":
    unittest.main()
