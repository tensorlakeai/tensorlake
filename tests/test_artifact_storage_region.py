import unittest
from unittest.mock import patch

from tensorlake.artifact_storage_region import resolve_artifact_storage_api_url
from tensorlake.filesystem import FilesystemClient
from tensorlake.repositories import RepositoryClient


class ArtifactStorageRegionTests(unittest.TestCase):
    def test_no_region_preserves_custom_endpoint(self):
        self.assertEqual(
            resolve_artifact_storage_api_url("http://localhost:8080"),
            "http://localhost:8080",
        )

    def test_invalid_region_or_endpoint_is_rejected(self):
        for region in ["", "eu-west-1", "EU-CENTRAL-1", "../us-east-1"]:
            with self.subTest(region=region), self.assertRaises(ValueError):
                resolve_artifact_storage_api_url("https://api.tensorlake.ai", region)
        for url in ["https://api.tensorlake.dev", "http://localhost:8080"]:
            with self.subTest(url=url), self.assertRaises(ValueError):
                resolve_artifact_storage_api_url(url, "eu-central-1")

    def test_filesystems_use_same_region_for_native_operations_and_mounts(self):
        with patch("tensorlake.filesystem.client.NativeFilesystems") as native:
            client = FilesystemClient(
                api_key="test",
                api_url="https://api.tensorlake.ai",
                region="eu-central-1",
            )
            self.assertEqual(
                native.call_args.kwargs["api_url"],
                "https://api.eu-central-1.tensorlake.ai",
            )
            self.assertEqual(
                client._cli._env_overrides["TENSORLAKE_API_URL"],
                native.call_args.kwargs["api_url"],
            )

    def test_repository_clients_have_independent_regions(self):
        with patch("tensorlake.repositories.CloudClient") as native:
            for region, url in [
                ("eu-central-1", "https://api.eu-central-1.tensorlake.ai"),
                ("us-east-1", "https://api.tensorlake.ai"),
            ]:
                RepositoryClient(
                    api_key="test", api_url="https://api.tensorlake.ai", region=region
                )
                self.assertEqual(native.call_args.kwargs["api_url"], url)


if __name__ == "__main__":
    unittest.main()
