"""Error handling utilities for the CLI."""

from __future__ import annotations

import sys
import traceback

import click
import httpx


def handle_http_error(
    e: httpx.HTTPStatusError, ctx: Context, operation: str = "request"
) -> None:
    """
    Handle HTTP errors with user-friendly messages.

    Args:
        e: The HTTP status error
        ctx: The CLI context
        operation: Description of what operation failed (e.g., "fetching secrets")
    """
    status_code = e.response.status_code

    # Show user-friendly error message based on status code
    if status_code == 401:
        _handle_unauthorized_error(ctx)
    elif status_code == 403:
        _handle_forbidden_error(ctx, operation)
    elif status_code == 404:
        _handle_not_found_error(operation)
    elif status_code >= 500:
        _handle_server_error(e, operation)
    else:
        _handle_generic_error(e, operation)

    # Show technical details in debug mode or hint about debug mode
    if ctx.debug:
        click.echo("", err=True)
        click.echo("Technical details:", err=True)
        click.echo(f"  Status: {status_code} {e.response.reason_phrase}", err=True)
        click.echo(f"  URL: {e.request.url}", err=True)
        if e.response.text:
            click.echo(f"  Response: {e.response.text}", err=True)
        click.echo("", err=True)
        click.echo("Stack trace:", err=True)
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
    else:
        click.echo("", err=True)
        click.echo(
            "For technical details and stack trace, run with --debug or set TENSORLAKE_DEBUG=1",
            err=True,
        )

    raise click.ClickException(f"{operation} failed with status {status_code}")


def _handle_unauthorized_error(ctx: Context) -> None:
    """Handle 401 Unauthorized errors."""
    click.echo(
        "Authentication failed: Your credentials are invalid or expired.", err=True
    )
    click.echo("", err=True)

    if ctx.api_key:
        click.echo("You're using an API key that is no longer valid.", err=True)
        click.echo(
            "Please check your API key or use 'tensorlake login' instead.", err=True
        )
    else:
        click.echo("Please run 'tensorlake login' to re-authenticate.", err=True)


def _handle_forbidden_error(ctx: Context, operation: str) -> None:
    """Handle 403 Forbidden errors."""
    click.echo(f"Permission denied while {operation}.", err=True)
    click.echo("", err=True)

    # Show current configuration
    show_current_config(ctx)

    click.echo("", err=True)
    click.echo("This usually means:", err=True)
    click.echo("  • Your account doesn't have access to this project", err=True)
    click.echo("  • The organization or project ID is incorrect", err=True)
    click.echo("  • Your API key or token has insufficient permissions", err=True)
    click.echo("", err=True)
    click.echo("To fix:", err=True)
    click.echo("  1. Run 'tensorlake init' to reconfigure your project", err=True)
    click.echo(f"  2. Or verify your permissions at {ctx.cloud_url}", err=True)


def _handle_not_found_error(operation: str) -> None:
    """Handle 404 Not Found errors."""
    click.echo(f"Resource not found while {operation}.", err=True)
    click.echo("", err=True)
    click.echo("The requested resource doesn't exist or has been deleted.", err=True)
    click.echo("Please check your configuration and try again.", err=True)


def _handle_server_error(e: httpx.HTTPStatusError, operation: str) -> None:
    """Handle 5xx server errors."""
    click.echo(f"Service error while {operation}.", err=True)
    click.echo("", err=True)
    click.echo(
        f"The server returned an error: {e.response.status_code} {e.response.reason_phrase}",
        err=True,
    )
    click.echo(
        "This is usually a temporary issue. Please try again in a few moments.",
        err=True,
    )
    click.echo("", err=True)
    click.echo("If the problem persists, please contact support.", err=True)


def _handle_generic_error(e: httpx.HTTPStatusError, operation: str) -> None:
    """Handle generic HTTP errors."""
    click.echo(f"Request failed while {operation}.", err=True)
    click.echo("", err=True)
    click.echo(
        f"The server returned: {e.response.status_code} {e.response.reason_phrase}",
        err=True,
    )

    # Try to extract error message from response
    try:
        error_data = e.response.json()
        if "message" in error_data:
            click.echo(f"Error: {error_data['message']}", err=True)
    except Exception:
        pass


def show_current_config(ctx: Context) -> None:
    """
    Display current configuration with sources.

    Args:
        ctx: The CLI context
    """
    click.echo("Current configuration:", err=True)
    click.echo(f"  Endpoint: {ctx.api_url}", err=True)

    org_source = ctx.get_organization_source()
    proj_source = ctx.get_project_source()

    click.echo(
        f"  Organization: {ctx.organization_id} (from {org_source})",
        err=True,
    )
    click.echo(
        f"  Project: {ctx.project_id} (from {proj_source})",
        err=True,
    )

    if ctx.api_key:
        click.echo("  Auth: API Key", err=True)
    elif ctx.personal_access_token:
        click.echo("  Auth: Personal Access Token", err=True)
    else:
        click.echo("  Auth: None", err=True)


def format_suggestion(message: str) -> str:
    """
    Format a suggestion message for consistent display.

    Args:
        message: The suggestion text

    Returns:
        Formatted suggestion string
    """
    return f"💡 {message}"
