import importlib.metadata
import sys
from dataclasses import dataclass
from typing import Literal

import click
import httpx

from tensorlake.applications.remote.api_client import APIClient
from tensorlake.cli._configuration import (
    get_nested_value,
    load_config,
    load_credentials,
    load_local_config,
)
from tensorlake.utils.http_client import (
    EventHook,
)

try:
    VERSION = importlib.metadata.version("tensorlake")
except importlib.metadata.PackageNotFoundError:
    VERSION = "unknown"


def raise_on_authn_authz(response: httpx.Response):
    if response.status_code == 401:
        raise click.UsageError(
            "The credentials to access Tensorlake's API are not valid"
        )
    elif response.status_code == 403:
        raise click.UsageError(
            "The credentials to access Tensorlake's API are not authorized for this operation"
        )


async def raise_on_authn_authz_async(response: httpx.Response):
    raise_on_authn_authz(response)


HTTP_EVENT_HOOKS = {
    "response": [raise_on_authn_authz],
}

ASYNC_HTTP_EVENT_HOOKS = {
    "response": [raise_on_authn_authz_async],
}


@dataclass
class Context:
    """Class for CLI context."""

    api_url: str
    cloud_url: str
    namespace: str
    api_key: str | None = None
    personal_access_token: str | None = None
    version: str = VERSION
    debug: bool = False
    _client: httpx.Client | None = None
    _introspect_response: httpx.Response | None = None
    _api_client: APIClient | None = None
    # Organization and project ID with source tracking
    organization_id_value: str | None = None
    organization_id_source: Literal["cli", "config"] | None = None
    project_id_value: str | None = None
    project_id_source: Literal["cli", "config"] | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            headers = {
                "Accept": "application/json",
                "User-Agent": f"Tensorlake CLI (python/{sys.version_info[0]}.{sys.version_info[1]} sdk/{self.version})",
            }
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            elif self.personal_access_token:
                headers["Authorization"] = f"Bearer {self.personal_access_token}"
                # Add X-Forwarded headers when using PAT (not needed for API keys)
                if self.organization_id:
                    headers["X-Forwarded-Organization-Id"] = self.organization_id
                if self.project_id:
                    headers["X-Forwarded-Project-Id"] = self.project_id
            else:
                raise click.UsageError(
                    "Missing API key or personal access token. Please run `tensorlake login` to authenticate."
                )

            self._client = httpx.Client(
                base_url=self.api_url, headers=headers, event_hooks=HTTP_EVENT_HOOKS
            )
        return self._client

    @property
    def api_client(self) -> APIClient:
        if self._api_client is None:
            # Determine which authentication method to use
            # IMPORTANT: Only pass api_key if using actual API key (not PAT)
            # This ensures X-Forwarded headers are set correctly for PAT
            if self.api_key:
                # Using API key - pass it and let server get org/project via introspection
                bearer_token = self.api_key
                org_id = None
                proj_id = None
            else:
                # Using PAT - pass PAT as api_key AND org/project for X-Forwarded headers
                bearer_token = self.personal_access_token
                org_id = self.organization_id
                proj_id = self.project_id

            self._api_client = APIClient(
                api_url=self.api_url,
                api_key=bearer_token,
                organization_id=org_id,
                project_id=proj_id,
                namespace=self.namespace,
                event_hooks=HTTP_EVENT_HOOKS,
            )

        return self._api_client

    @property
    def api_key_id(self):
        if self.api_key:
            return self._introspect().json().get("id")
        else:
            return None

    @property
    def project_id(self):
        """
        Get the project ID associated with the API key, or from config if no API key is set.
        """
        if self.api_key:
            return self._introspect().json().get("projectId")

        return self.project_id_value

    @property
    def organization_id(self):
        """
        Get the organization ID associated with the API key, or from config if no API key is set.
        """
        if self.api_key:
            return self._introspect().json().get("organizationId")

        return self.organization_id_value

    def _introspect(self) -> httpx.Response:
        if self._introspect_response is None:
            try:
                introspect_response = self.client.post("/platform/v1/keys/introspect")
                if introspect_response.status_code == 401:
                    click.echo(
                        "The TensorLake API key is not valid.",
                        err=True,
                    )
                    click.echo(
                        "Please supply a valid API key with the `--api-key` flag, or run `tensorlake login` to authenticate.",
                        err=True,
                    )
                    raise click.ClickException("Invalid API key")
                if introspect_response.status_code == 404:
                    click.echo(
                        f"The server at {self.api_url} doesn't support TensorLake API introspection.",
                        err=True,
                    )
                    click.echo(
                        "Please check your API URL or contact support.",
                        err=True,
                    )
                    raise click.ClickException("API introspection not supported")
                introspect_response.raise_for_status()
                self._introspect_response = introspect_response
            except httpx.HTTPStatusError as e:
                # Handle other HTTP errors inline to avoid circular import
                status_code = e.response.status_code
                click.echo(f"Error validating API key: HTTP {status_code}", err=True)

                if self.debug:
                    click.echo("", err=True)
                    click.echo("Technical details:", err=True)
                    click.echo(
                        f"  Status: {status_code} {e.response.reason_phrase}", err=True
                    )
                    click.echo(f"  URL: {e.request.url}", err=True)
                    if e.response.text:
                        click.echo(f"  Response: {e.response.text}", err=True)
                else:
                    click.echo("", err=True)
                    click.echo(
                        "For technical details, run with --debug or set TENSORLAKE_DEBUG=1",
                        err=True,
                    )

                raise click.ClickException(
                    f"API key validation failed with status {status_code}"
                )
        return self._introspect_response

    def has_authentication(self) -> bool:
        """Check if any form of authentication is available."""
        return self.api_key is not None or self.personal_access_token is not None

    def has_org_and_project(self) -> bool:
        """Check if organization and project IDs are available from any source."""
        return self.organization_id is not None and self.project_id is not None

    def needs_init(self) -> bool:
        """
        Check if init flow should run.
        Init is needed if we have PAT but no org/project IDs.
        If using API key, org/project come from introspection, so init not needed.
        """
        if self.api_key:
            # API key provides org/project via introspection
            return False

        if not self.has_authentication():
            # No auth at all - need login first, not init
            return False

        # Have PAT but missing org/project
        return not self.has_org_and_project()

    @property
    def organization_id_from_cli(self) -> str | None:
        """Get organization ID if it was provided via CLI, otherwise None."""
        if self.organization_id_source == "cli":
            return self.organization_id_value
        return None

    @property
    def project_id_from_cli(self) -> str | None:
        """Get project ID if it was provided via CLI, otherwise None."""
        if self.project_id_source == "cli":
            return self.project_id_value
        return None

    def get_organization_source(self) -> str:
        """
        Get the source of the organization ID.

        Returns:
            Description of where the organization ID came from
        """
        if self.api_key:
            return "API key introspection"

        if self.organization_id_source == "cli":
            return "CLI flag or environment variable"

        if self.organization_id_source == "config":
            return "local config (.tensorlake/config.toml)"

        return "not configured"

    def get_project_source(self) -> str:
        """
        Get the source of the project ID.

        Returns:
            Description of where the project ID came from
        """
        if self.api_key:
            return "API key introspection"

        if self.project_id_source == "cli":
            return "CLI flag or environment variable"

        if self.project_id_source == "config":
            return "local config (.tensorlake/config.toml)"

        return "not configured"

    @classmethod
    def _resolve_api_url(
        cls, api_url: str | None, local_config: dict, global_config: dict
    ) -> str:
        """Resolve base URL from CLI args, config, or default."""
        return (
            api_url
            or get_nested_value(local_config, "tensorlake.api_url")
            or get_nested_value(global_config, "tensorlake.api_url")
            or "https://api.tensorlake.ai"
        )

    @classmethod
    def _resolve_cloud_url(
        cls,
        cloud_url: str | None,
        api_url: str,
        local_config: dict,
        global_config: dict,
    ) -> str:
        """Resolve cloud URL from CLI args, config, or default."""
        return (
            cloud_url
            or get_nested_value(local_config, "tensorlake.cloud_url")
            or get_nested_value(global_config, "tensorlake.cloud_url")
            or cls._resolve_cloud_url_from_api_url(api_url)
        )

    @classmethod
    def _resolve_cloud_url_from_api_url(cls, api_url: str) -> str:
        """Resolve cloud URL from API URL."""
        return (
            api_url.replace("https://api.tensorlake.", "https://cloud.tensorlake.")
            if api_url.startswith("https://api.tensorlake.")
            else "https://cloud.tensorlake.ai"
        )

    @classmethod
    def _resolve_authentication(
        cls,
        api_key: str | None,
        personal_access_token: str | None,
        local_config: dict,
        global_config: dict,
        api_url: str,
    ) -> tuple[str | None, str | None]:
        """Resolve API key and PAT from CLI args, config, or credentials file."""
        final_api_key = (
            api_key
            or get_nested_value(local_config, "tensorlake.apikey")
            or get_nested_value(global_config, "tensorlake.apikey")
        )

        # Load PAT from credentials file (endpoint-scoped) if not provided via CLI/env
        file_personal_access_token = load_credentials(api_url)

        # Priority: CLI/env PAT > credentials file PAT
        final_personal_access_token = (
            personal_access_token or file_personal_access_token
        )

        return final_api_key, final_personal_access_token

    @classmethod
    def _resolve_namespace(
        cls, namespace: str | None, local_config: dict, global_config: dict
    ) -> str:
        """Resolve namespace from CLI args, config, or default."""
        return (
            namespace
            or get_nested_value(local_config, "indexify.namespace")
            or get_nested_value(global_config, "indexify.namespace")
            or "default"
        )

    @classmethod
    def _resolve_project_config(
        cls, organization_id: str | None, project_id: str | None, local_config: dict
    ) -> tuple[
        str | None,
        Literal["cli", "config"] | None,
        str | None,
        Literal["cli", "config"] | None,
    ]:
        """Resolve organization and project IDs from CLI args or local config.

        Note: Organization and project IDs are NOT loaded from global config.
        They must come from CLI flags, env vars, or local .tensorlake/config.toml only.

        Returns:
            tuple of (org_id, org_source, project_id, project_source)
        """
        # Resolve organization ID and track source
        if organization_id:
            final_organization_id = organization_id
            org_source = "cli"
        else:
            final_organization_id = get_nested_value(local_config, "organization")
            org_source = "config" if final_organization_id else None

        # Resolve project ID and track source
        if project_id:
            final_project_id = project_id
            proj_source = "cli"
        else:
            final_project_id = get_nested_value(local_config, "project")
            proj_source = "config" if final_project_id else None

        return final_organization_id, org_source, final_project_id, proj_source

    @classmethod
    def default(
        cls,
        api_url: str | None = None,
        cloud_url: str | None = None,
        api_key: str | None = None,
        personal_access_token: str | None = None,
        namespace: str | None = None,
        organization_id: str | None = None,
        project_id: str | None = None,
        debug: bool = False,
    ) -> "Context":
        """Create a Context with values from CLI args, environment, saved config, or defaults."""
        # Load both local and global config
        local_config_data = load_local_config()
        global_config_data = load_config()

        # Resolve all configuration values using helper methods
        final_api_url = cls._resolve_api_url(
            api_url, local_config_data, global_config_data
        )
        final_cloud_url = cls._resolve_cloud_url(
            cloud_url, final_api_url, local_config_data, global_config_data
        )
        final_api_key, final_personal_access_token = cls._resolve_authentication(
            api_key,
            personal_access_token,
            local_config_data,
            global_config_data,
            final_api_url,
        )
        final_namespace = cls._resolve_namespace(
            namespace, local_config_data, global_config_data
        )
        (
            final_organization_id,
            org_source,
            final_project_id,
            proj_source,
        ) = cls._resolve_project_config(organization_id, project_id, local_config_data)

        return cls(
            api_url=final_api_url,
            cloud_url=final_cloud_url,
            api_key=final_api_key,
            personal_access_token=final_personal_access_token,
            namespace=final_namespace,
            debug=debug,
            organization_id_value=final_organization_id,
            organization_id_source=org_source,
            project_id_value=final_project_id,
            project_id_source=proj_source,
        )


"""Pass the Context object to the click command"""
pass_auth = click.make_pass_decorator(Context)


def require_auth_and_project(f):
    """
    Decorator that ensures authentication and org/project IDs are available.

    This decorator:
    1. Checks if authentication exists (API key or PAT)
    2. If using API key, org/project come from introspection automatically
    3. If using PAT but no org/project, automatically runs init flow
    4. If no auth at all, shows error message to run 'tensorlake login'

    Usage:
        @click.command()
        @require_auth_and_project
        def my_command(ctx: Context):
            # ctx.organization_id and ctx.project_id are guaranteed to be available
            pass
    """
    import functools

    @pass_auth
    @functools.wraps(f)
    def wrapper(ctx: Context, *args, **kwargs):
        # Check if we have any authentication
        if not ctx.has_authentication():
            # No authentication - automatically run login flow
            click.echo("It seems like you're not logged in. Let's log you in...\n")

            # Import here to avoid circular dependency
            from tensorlake.cli.auth import run_login_flow

            try:
                # Run login flow, which will also run init if needed
                run_login_flow(ctx, auto_init=True)

                # Reload context with new credentials
                # The login flow will have saved credentials and potentially created local config
                updated_ctx = Context.default(
                    api_url=ctx.api_url,
                    cloud_url=ctx.cloud_url,
                    namespace=ctx.namespace,
                    # Let Context.default reload everything from saved credentials and config
                )

                # Verify authentication is now available
                if not updated_ctx.has_authentication():
                    raise click.UsageError(
                        "Authentication failed. Please try running 'tensorlake login' manually."
                    )

                # Verify org/project are now available
                if not updated_ctx.has_org_and_project():
                    raise click.UsageError(
                        "Organization and project configuration missing. Please run 'tensorlake init'."
                    )

                # Continue with the command using the updated context
                return f(updated_ctx, *args, **kwargs)

            except click.Abort:
                click.echo(
                    "\nAuthentication cancelled. Please run 'tensorlake login' to authenticate.",
                    err=True,
                )
                raise

        # If using API key, org/project come from introspection
        # If already have org/project from any source, we're good
        if ctx.has_org_and_project():
            return f(ctx, *args, **kwargs)

        # At this point: we have auth but no org/project
        # If using API key, something is wrong (should have gotten them from introspection)
        if ctx.api_key:
            raise click.UsageError(
                "API key is set but could not determine organization and project. "
                "Please check your API key or provide --organization and --project flags."
            )

        # We have PAT but no org/project - need to run init
        click.echo("Organization and project IDs are required for this command.")
        click.echo("Running initialization flow to set up your project...\n")

        # Import here to avoid circular dependency
        from tensorlake.cli._project_detection import find_project_root
        from tensorlake.cli.init import run_init_flow

        # Detect project root automatically
        project_root = find_project_root()

        try:
            org_id, proj_id = run_init_flow(
                ctx,
                interactive=True,
                create_local_config=True,
                skip_if_provided=False,
                project_root=project_root,
            )

            # Create a new context with the org/project IDs
            updated_ctx = Context.default(
                api_url=ctx.api_url,
                cloud_url=ctx.cloud_url,
                api_key=ctx.api_key,
                personal_access_token=ctx.personal_access_token,
                namespace=ctx.namespace,
                organization_id=org_id,
                project_id=proj_id,
            )

            return f(updated_ctx, *args, **kwargs)
        except click.Abort:
            click.echo(
                "\nInitialization aborted. Please run 'tensorlake init' to complete setup.",
                err=True,
            )
            raise

    return wrapper


class AliasedGroup(click.Group):
    """
    A Click Group that supports command aliases through prefix matching.

    This allows users to type abbreviated commands as long as they are unambiguous.
    For example, 'secrets' can be invoked as 'sec', 'deploy' as 'dep', etc.

    Example:
        tensorlake sec set   -> tensorlake secrets set
        tensorlake dep       -> tensorlake deploy
        tensorlake par       -> tensorlake parse
    """

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        """
        Get a command by name or prefix.

        First tries exact match, then falls back to prefix matching.
        If multiple commands match the prefix, returns None (ambiguous).
        """
        # Try exact match first
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv

        # Try prefix matching
        matches = [
            x for x in self.list_commands(ctx) if x.lower().startswith(cmd_name.lower())
        ]

        if not matches:
            return None

        if len(matches) == 1:
            return super().get_command(ctx, matches[0])

        # Multiple matches - ambiguous
        ctx.fail(
            f"Ambiguous command '{cmd_name}'. Could be: {', '.join(sorted(matches))}"
        )

    def resolve_command(
        self, ctx: click.Context, args: list[str]
    ) -> tuple[str, click.Command, list[str]]:
        """
        Resolve command name to always return the full command name, not the alias.

        This ensures that when users type 'tensorlake app list', the help text
        and error messages show 'application' instead of 'app'.
        """
        _, cmd, args = super().resolve_command(ctx, args)
        return cmd.name, cmd, args
