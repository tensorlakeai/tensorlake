import json
import os
import unittest
from unittest.mock import patch

from tensorlake.repositories import RepositoryClient, RepositoryError


class _FakeCloudClient:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.closed = False
        self.calls = []
        _FakeCloudClient.instances.append(self)

    def close(self):
        self.closed = True

    def introspect_api_key_json(self):
        self.calls.append(("introspect",))
        return json.dumps({"project_id": "project-from-key"})

    def git_repo_url(self, project_id, repo):
        self.calls.append(("url", project_id, repo))
        return f"https://git.tensorlake.ai/{project_id}/{repo}"

    def create_git_repo(self, project_id, repo, default_branch):
        self.calls.append(("create", project_id, repo, default_branch))
        return json.dumps(
            {
                "repo": repo,
                "url": f"https://git.tensorlake.ai/{project_id}/{repo}",
                "trace_id": "tr-create",
            }
        )

    def list_git_repos(self, project_id):
        self.calls.append(("list", project_id))
        return json.dumps(
            {
                "project": project_id,
                "repos": [
                    {
                        "name": "linux",
                        "full_name": f"{project_id}/linux",
                        "default_branch": "main",
                        "status": "active",
                    }
                ],
            }
        )

    def git_repo_info(self, project_id, repo):
        self.calls.append(("info", project_id, repo))
        return json.dumps(
            {
                "repo": repo,
                "url": f"https://git.tensorlake.ai/{project_id}/{repo}",
                "branches": [
                    {"name": "main", "ref_name": "refs/heads/main", "oid": "abc"}
                ],
                "refs": [{"name": "HEAD", "oid": "abc"}],
            }
        )

    def git_credential(self, project_id, repo):
        self.calls.append(("credential", project_id, repo))
        return json.dumps(
            {
                "token": "tok",
                "tokenType": "bearer",
                "expiresAt": "",
                "gitUsername": "t",
                "repoPattern": repo or "*",
                "scopes": [],
            }
        )

    def push_git_worktree(
        self,
        project_id,
        repo,
        root,
        branch,
        message,
        expect_oid,
    ):
        self.calls.append(("push", project_id, repo, root, branch, message, expect_oid))
        return json.dumps(
            {
                "commit": "def",
                "tree": "tree",
                "ref_name": f"refs/heads/{branch}",
                "created": False,
                "files": 1,
                "bytes_total": 12,
                "chunks_total": 1,
                "chunks_uploaded": 1,
                "bytes_uploaded": 12,
                "file_blob_oids": [["README.md", "oid"]],
            }
        )

    def merge_git_repo(
        self,
        project_id,
        repo,
        ours,
        theirs,
        preflight,
        deep,
        materialize,
        message,
        base,
    ):
        self.calls.append(
            (
                "merge",
                project_id,
                repo,
                ours,
                theirs,
                preflight,
                deep,
                materialize,
                message,
                base,
            )
        )
        return json.dumps(
            {
                "ours": "ours-oid",
                "theirs": "theirs-oid",
                "merge_base": "base-oid",
                "clean": False,
                "fast_forward": False,
                "already_merged": False,
                "changed_paths": 2,
                "conflicts": [
                    {
                        "path": "src/main.rs",
                        "kind": "content",
                        "potential": True,
                        "ours": {"mode": 33188, "oid": "ours-blob"},
                        "base": None,
                        "theirs": {"mode": 33188, "oid": "theirs-blob"},
                    }
                ],
                "stats": {
                    "trees_read": 3,
                    "entries_compared": 4,
                    "blobs_merged": 1,
                    "wall_ms": 2.5,
                },
                "commit": None,
                "fast_forwarded": False,
            }
        )

    def git_commit_conflicts(self, project_id, repo, commit):
        self.calls.append(("commit_conflicts", project_id, repo, commit))
        if commit == "clean-oid":
            return "null"
        return json.dumps(
            {
                "version": 1,
                "ours_commit": "ours-oid",
                "theirs_commit": "theirs-oid",
                "base_commit": None,
                "paths": [
                    {
                        "path": "src/main.rs",
                        "kind": "content",
                        "terms": [None, {"mode": 33188, "oid": "base-blob"}, None],
                    }
                ],
                "truncated_paths": 0,
            }
        )


class TestRepositoryClient(unittest.TestCase):
    def setUp(self):
        _FakeCloudClient.instances = []
        self._env = patch.dict(
            os.environ,
            {
                "TENSORLAKE_API_KEY": "k",
                "TENSORLAKE_PROJECT_ID": "project-1",
                "TENSORLAKE_ORGANIZATION_ID": "org-1",
            },
            clear=False,
        )
        self._env.start()
        self._client_patch = patch(
            "tensorlake.repositories.CloudClient", _FakeCloudClient
        )
        self._client_patch.start()

    def tearDown(self):
        self._client_patch.stop()
        self._env.stop()

    def test_create_repository(self):
        client = RepositoryClient()
        created = client.create("linux", default_branch="main")

        fake = _FakeCloudClient.instances[-1]
        self.assertEqual(
            fake.calls,
            [("create", "project-1", "linux", "main")],
        )
        self.assertEqual(created.repo, "linux")
        self.assertEqual(created.trace_id, "tr-create")

    def test_list_repositories(self):
        client = RepositoryClient()
        repos = client.list()

        self.assertEqual(repos[0].name, "linux")
        self.assertEqual(repos[0].full_name, "project-1/linux")
        self.assertEqual(repos[0].default_branch, "main")

    def test_info_and_credential(self):
        client = RepositoryClient()

        info = client.info("linux")
        credential = client.credential("linux")

        self.assertEqual(info.branches[0].ref_name, "refs/heads/main")
        self.assertEqual(info.refs[0].name, "HEAD")
        self.assertEqual(credential.git_username, "t")
        self.assertEqual(credential.repo_pattern, "linux")

    def test_push_worktree(self):
        client = RepositoryClient()
        report = client.push_worktree(
            "linux",
            root="/tmp/linux",
            branch="feature",
            message="sync",
            expect_oid="abc",
        )

        fake = _FakeCloudClient.instances[-1]
        self.assertEqual(
            fake.calls,
            [("push", "project-1", "linux", "/tmp/linux", "feature", "sync", "abc")],
        )
        self.assertEqual(report.ref_name, "refs/heads/feature")
        self.assertEqual(report.bytes_uploaded, 12)

    def test_merge(self):
        client = RepositoryClient()
        report = client.merge(
            "linux",
            "main",
            "feature",
            preflight=True,
            deep=True,
            message="merge feature",
            base="base-oid",
        )

        fake = _FakeCloudClient.instances[-1]
        self.assertEqual(
            fake.calls,
            [
                (
                    "merge",
                    "project-1",
                    "linux",
                    "main",
                    "feature",
                    True,
                    True,
                    False,
                    "merge feature",
                    "base-oid",
                )
            ],
        )
        self.assertEqual(report.merge_base, "base-oid")
        self.assertEqual(report.changed_paths, 2)
        self.assertTrue(report.conflicts[0].potential)
        self.assertIsNone(report.conflicts[0].base)
        self.assertEqual(report.stats.entries_compared, 4)

    def test_commit_conflicts(self):
        client = RepositoryClient()
        record = client.commit_conflicts("linux", "merge-oid")
        clean = client.commit_conflicts("linux", "clean-oid")

        fake = _FakeCloudClient.instances[-1]
        self.assertEqual(
            fake.calls,
            [
                ("commit_conflicts", "project-1", "linux", "merge-oid"),
                ("commit_conflicts", "project-1", "linux", "clean-oid"),
            ],
        )
        self.assertIsNotNone(record)
        assert record is not None
        self.assertEqual(record.ours_commit, "ours-oid")
        self.assertIsNone(record.base_commit)
        self.assertIsNone(record.paths[0].terms[0])
        self.assertEqual(record.paths[0].terms[1].oid, "base-blob")
        self.assertIsNone(clean)

    def test_derives_project_context_from_api_key(self):
        with patch.dict(os.environ, {"TENSORLAKE_PROJECT_ID": ""}, clear=False):
            client = RepositoryClient()
            created = client.create("linux")

        fake = _FakeCloudClient.instances[-1]
        self.assertEqual(
            fake.calls,
            [("introspect",), ("create", "project-from-key", "linux", None)],
        )
        self.assertEqual(
            created.url, "https://git.tensorlake.ai/project-from-key/linux"
        )

    def test_rejects_pat_without_api_key(self):
        with patch.dict(
            os.environ,
            {
                "TENSORLAKE_API_KEY": "",
                "TENSORLAKE_PAT": "pat",
                "TENSORLAKE_PROJECT_ID": "",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(
                RepositoryError,
                "Personal access tokens are CLI-only",
            ):
                RepositoryClient()

    def test_requires_api_key(self):
        with patch.dict(
            os.environ,
            {
                "TENSORLAKE_API_KEY": "",
                "TENSORLAKE_PAT": "",
                "TENSORLAKE_PROJECT_ID": "",
            },
            clear=False,
        ):
            with self.assertRaisesRegex(
                RepositoryError,
                "Missing TENSORLAKE_API_KEY",
            ):
                RepositoryClient()


if __name__ == "__main__":
    unittest.main()
