"""Single Python wrapper around the Rust Cloud SDK (tensorlake._cloud_sdk).

All Python code that needs to communicate with the TensorLake Cloud API
should use CloudClient. The Rust SDK handles HTTP, auth, and serialization.
"""

import importlib

from tensorlake._tracing import USER_AGENT

_IMPORT_ERROR: Exception | None = None
_RustClient = None
_RustClientError = None
_AVAILABLE = False

for _module_name in ("tensorlake._cloud_sdk", "_cloud_sdk"):
    try:
        _mod = importlib.import_module(_module_name)
        _RustClient = _mod.CloudApiClient
        _RustClientError = _mod.CloudApiClientError
        _AVAILABLE = True
        _IMPORT_ERROR = None
        break
    except Exception as e:
        _IMPORT_ERROR = e

_API_KEY_ENV_VAR = "TENSORLAKE_API_KEY"


def _raise_as_tensorlake_error(e: Exception) -> None:
    """Convert Rust SDK exceptions into the TensorlakeError hierarchy."""
    # Lazy import to avoid circular dependency with applications package.
    from tensorlake.applications.interface.exceptions import (
        InternalError,
        RemoteAPIError,
        SDKUsageError,
        TensorlakeError,
    )

    if isinstance(e, TensorlakeError):
        raise

    if (
        _RustClientError is not None
        and isinstance(e, _RustClientError)
        and len(e.args) > 0
    ):
        kind: str | None = None
        status_code: int | None = None
        message: str = str(e)

        if len(e.args) == 3:
            kind, status_code, message = e.args
        elif len(e.args) == 1 and isinstance(e.args[0], tuple) and len(e.args[0]) == 3:
            kind, status_code, message = e.args[0]

        if status_code == 401:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not valid. "
                f"Please check your `tensorlake login` status or '{_API_KEY_ENV_VAR}' environment variable."
            ) from None
        elif status_code == 403:
            raise SDKUsageError(
                "The provided Tensorlake API credentials are not authorized for the requested operation."
            ) from None
        elif status_code is not None:
            raise RemoteAPIError(status_code=status_code, message=message) from None
        elif kind == "connection":
            raise InternalError(
                f"Connection error while communicating with Tensorlake API: {message}"
            ) from None
        elif kind == "sdk_usage":
            raise SDKUsageError(message) from None
        else:
            raise InternalError(message) from e

    raise InternalError(str(e)) from e


class CloudClient:
    """Thin wrapper around the Rust Cloud SDK PyO3 client.

    Provides consistent error handling: all Rust SDK exceptions are converted
    into the TensorlakeError hierarchy.
    """

    def __init__(
        self,
        api_url: str,
        api_key: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
        namespace: str | None = None,
    ):
        if not _AVAILABLE:
            from tensorlake.applications.interface.exceptions import InternalError

            details = (
                f" Import error: {type(_IMPORT_ERROR).__name__}: {_IMPORT_ERROR}"
                if _IMPORT_ERROR is not None
                else ""
            )
            raise InternalError(
                "Rust Cloud SDK client is required but unavailable. "
                f"Build/install it with `make build_rust_py_client`.{details}"
            )
        try:
            self._client = _RustClient(
                api_url=api_url,
                api_key=api_key,
                organization_id=organization_id,
                project_id=project_id,
                namespace=namespace,
                user_agent=USER_AGENT,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def close(self):
        self._client.close()

    def __enter__(self) -> "CloudClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    # -- Application operations --

    def upsert_application(
        self,
        manifest_json: str,
        code_zip: bytes,
        upgrade_running_requests: bool,
    ) -> None:
        try:
            self._client.upsert_application(
                manifest_json=manifest_json,
                code_zip=code_zip,
                upgrade_running_requests=upgrade_running_requests,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def ensure_application_public_endpoint_json(
        self,
        application_name: str,
        allow: list[str],
    ) -> str:
        try:
            return self._client.ensure_application_public_endpoint_json(
                application_name=application_name,
                allow=allow,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def delete_application(self, application_name: str) -> None:
        try:
            self._client.delete_application(application_name=application_name)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def applications_json(self) -> str:
        try:
            return self._client.applications_json()
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def application_manifest_json(self, application_name: str) -> str:
        try:
            return self._client.application_manifest_json(
                application_name=application_name
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Request operations --

    def run_request(
        self,
        application_name: str,
        inputs: list[tuple[str, bytes, str]],
    ) -> str:
        try:
            return self._client.run_request(
                application_name=application_name,
                inputs=inputs,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def wait_on_request_completion(
        self,
        application_name: str,
        request_id: str,
    ) -> None:
        try:
            self._client.wait_on_request_completion(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def request_metadata_json(
        self,
        application_name: str,
        request_id: str,
    ) -> str:
        try:
            return self._client.request_metadata_json(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def request_output_bytes(
        self,
        application_name: str,
        request_id: str,
    ) -> tuple:
        try:
            return self._client.request_output_bytes(
                application_name=application_name,
                request_id=request_id,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Auth operations --

    def introspect_api_key_json(self) -> str:
        try:
            return self._client.introspect_api_key_json()
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Secrets operations --

    def list_secrets_json(
        self,
        organization_id: str,
        project_id: str,
        page_size: int = 100,
    ) -> str:
        try:
            return self._client.list_secrets_json(
                organization_id=organization_id,
                project_id=project_id,
                page_size=page_size,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    # -- Git repository operations --

    def git_repo_url(self, project_id: str, repo: str) -> str:
        try:
            return self._client.git_repo_url(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def create_git_repo(
        self,
        project_id: str,
        repo: str,
        default_branch: str | None = None,
    ) -> str:
        try:
            return self._client.create_git_repo(project_id, repo, default_branch)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def list_git_repos(self, project_id: str) -> str:
        try:
            return self._client.list_git_repos(project_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def delete_git_repo(self, project_id: str, repo: str) -> None:
        try:
            self._client.delete_git_repo(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def fork_git_repo(self, project_id: str, repo: str, base_repo: str) -> str:
        try:
            return self._client.fork_git_repo(project_id, repo, base_repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def archive_git_repo(self, project_id: str, repo: str) -> None:
        try:
            self._client.archive_git_repo(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def restore_git_repo(self, project_id: str, repo: str) -> None:
        try:
            self._client.restore_git_repo(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def git_repo_info(self, project_id: str, repo: str) -> str:
        try:
            return self._client.git_repo_info(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def list_git_branches(self, project_id: str, repo: str) -> str:
        try:
            return self._client.list_git_branches(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def list_git_refs(self, project_id: str, repo: str) -> str:
        try:
            return self._client.list_git_refs(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def delete_git_branch(self, project_id: str, repo: str, branch: str) -> None:
        try:
            self._client.delete_git_branch(project_id, repo, branch)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def list_git_operations(self, project_id: str, repo: str) -> str:
        try:
            return self._client.list_git_operations(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def git_credential(self, project_id: str, repo: str | None = None) -> str:
        try:
            return self._client.git_credential(project_id, repo)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def git_commit_status(self, project_id: str, repo: str, job_id: str) -> str:
        try:
            return self._client.git_commit_status(project_id, repo, job_id)
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def push_git_worktree(
        self,
        project_id: str,
        repo: str,
        root: str,
        branch: str,
        message: str,
        expect_oid: str | None = None,
    ) -> str:
        try:
            return self._client.push_git_worktree(
                project_id,
                repo,
                root,
                branch,
                message,
                expect_oid,
            )
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def merge_git_repo(
        self,
        project_id: str,
        repo: str,
        ours: str,
        theirs: str,
        preflight: bool = False,
        deep: bool = False,
        materialize: bool = False,
        message: str | None = None,
        base: str | None = None,
    ) -> str:
        try:
            return self._client.merge_git_repo(
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
        except Exception as e:
            _raise_as_tensorlake_error(e)

    def git_commit_conflicts(
        self,
        project_id: str,
        repo: str,
        commit: str,
    ) -> str:
        try:
            return self._client.git_commit_conflicts(project_id, repo, commit)
        except Exception as e:
            _raise_as_tensorlake_error(e)
