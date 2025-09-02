from typing import TypeVar, Generic, Type, Optional, Callable
from enum import Enum
import asyncio
import time

from pydantic import BaseModel, ValidationError
from rich.text import Text
from rich.live import Live
from httpx_sse import ServerSentEvent, aconnect_sse, connect_sse

from ._base import _BaseClient

T = TypeVar("T", bound=BaseModel)


class WaitableOperation(str, Enum):
    PARSE = "parse"
    READ = "read"
    CLASSIFY = "classify"
    EXTRACT = "extract"


class _CompletionWaiter(Generic[T], _BaseClient):

    def _base_wait_for_completion(
        self, entity_id: str, operation: WaitableOperation, result_model: Type[T]
    ) -> T:
        """
        Wait for the completion of an operation.

        Args:
            entity_id: The ID of the entity to wait for
            operation: The operation to wait for
            result_model: The Pydantic model class for the result

        Returns:
            Instance of result_model with the completed operation result
        """
        status_text = Text(
            f"Waiting for completion of {operation.value} job.", style="bold"
        )
        retry_count = 0

        with Live(
            status_text,
            refresh_per_second=4,
            transient=True,
            redirect_stdout=True,
            redirect_stderr=True,
        ) as live:
            # Print static info above the live line
            live.console.print(f"{operation.value.title()} ID: {entity_id}")

            def set_status(message: str, style: Optional[str] = None) -> None:
                live.update(Text(message, style=style), refresh=True)

            def print_line(message: str) -> None:
                live.console.print(message)

            while retry_count < 5:
                try:
                    with connect_sse(
                        client=self._client,
                        method="GET",
                        url=f"{operation.value}/{entity_id}",
                        headers=self._headers(),
                    ) as sse:
                        for sse_event in sse.iter_sse():
                            result = self._handle_sse_event(
                                sse_event,
                                set_status,
                                print_line,
                                result_model,
                                operation.value,
                            )
                            if result:
                                return result

                        live.console.print(
                            "[yellow]SSE connection ended without completion event[/yellow]"
                        )

                except (ConnectionError, TimeoutError) as e:
                    retry_count += 1
                    live.console.print(
                        f"[yellow]Connection issue (attempt {retry_count} / 5): {e}[/yellow]"
                    )
                    if retry_count < 5:
                        wait_time = min(2**retry_count, 30)
                        live.console.print(
                            f"[yellow]Retrying in {wait_time} seconds...[/yellow]"
                        )
                        time.sleep(wait_time)

            live.console.print(
                "[yellow]Max retries reached. Checking final status...[/yellow]"
            )

            final_result = self._base_get_result(entity_id)
            if hasattr(final_result, "status") and final_result.status:
                style = self._get_status_style(str(final_result.status))
                set_status(f"Status: {final_result.status}", style)
            return final_result

    async def _base_wait_for_completion_async(
        self, entity_id: str, operation: WaitableOperation, result_model: Type[T]
    ) -> T:
        status_text = Text(
            f"Waiting for completion of {operation.value} job.", style="bold"
        )
        retry_count = 0

        with Live(
            status_text,
            refresh_per_second=4,
            transient=True,
            redirect_stdout=True,
            redirect_stderr=True,
        ) as live:
            live.console.print(f"{operation.value.title()} ID: {entity_id}")

            def set_status(message: str, style: Optional[str] = None) -> None:
                live.update(Text(message, style=style), refresh=True)

            def print_line(message: str) -> None:
                live.console.print(message)

            while retry_count < 5:
                try:
                    async with aconnect_sse(
                        client=self._client,
                        method="GET",
                        url=f"{operation.value}/{entity_id}",
                        headers=self._headers(),
                    ) as sse:
                        async for sse_event in sse.aiter_sse():
                            result = self._handle_sse_event(
                                sse_event,
                                set_status,
                                print_line,
                                result_model,
                                operation.value,
                            )
                            if result:
                                return result

                            await asyncio.sleep(0)

                        live.console.print(
                            "[yellow]SSE connection ended without completion event[/yellow]"
                        )
                except (ConnectionError, TimeoutError) as e:
                    retry_count += 1
                    live.console.print(
                        f"[yellow]Connection issue (attempt {retry_count} / 5): {e}[/yellow]"
                    )
                    if retry_count < 5:
                        wait_time = min(2**retry_count, 30)
                        live.console.print(
                            f"[yellow]Retrying in {wait_time} seconds...[/yellow]"
                        )
                        await asyncio.sleep(wait_time)

            live.console.print(
                "[yellow]Max retries reached. Checking final status...[/yellow]"
            )
            final_result = await self._base_get_result_async(entity_id)
            if hasattr(final_result, "status") and final_result.status:
                style = self._get_status_style(str(final_result.status))
                set_status(f"Status: {final_result.status}", style)
            return final_result

    def _handle_sse_event(
        self,
        sse_event: ServerSentEvent,
        set_status: Callable[[str, Optional[str]], None],
        print_line: Callable[[str], None],
        result_model: Type[T],
        operation_name: str,
    ) -> Optional[T]:
        """
        Handle SSE event and return result if operation is complete.
        """
        # Map generic event patterns to specific operations
        event_patterns = {
            f"{operation_name}_update": ("magenta", None),
            f"{operation_name}_done": ("green", True),
            f"{operation_name}_failed": ("red", True),
            f"{operation_name}_queued": ("yellow", None),
        }

        # Handle the event
        if sse_event.event in event_patterns:
            (style,) = event_patterns[sse_event.event]

            try:
                result = result_model.model_validate_json(sse_event.data)

                if sse_event.event.endswith("_update"):
                    status_value = getattr(result, "status", "processing")
                    if hasattr(status_value, "value"):
                        status_value = status_value.value
                    set_status(f"Status: {status_value}", style)

                elif sse_event.event.endswith("_done"):
                    operation_id = getattr(
                        result, f"{operation_name}_id", getattr(result, "id", "unknown")
                    )
                    set_status(
                        f"{operation_name.title()} ID: {operation_id} done", style
                    )
                    return result

                elif sse_event.event.endswith("_failed"):
                    operation_id = getattr(
                        result, f"{operation_name}_id", getattr(result, "id", "unknown")
                    )
                    error = getattr(result, "error", None)
                    message = (
                        f"{operation_name.title()} failed ({operation_id}): {error}"
                        if error
                        else f"{operation_name.title()} failed ({operation_id})"
                    )
                    set_status(message, style)
                    print_line(f"[red]{message}[/red]")
                    return result

                elif sse_event.event.endswith("_queued"):
                    set_status(f"{operation_name.title()} job waiting in queue.", style)

            except ValidationError:
                set_status(
                    f"{operation_name.title()} update received: {sse_event.data}",
                    "blue",
                )

        else:
            set_status(f"Unknown SSE event: {sse_event.event}", "cyan")

        return None

    def _get_status_style(self, status: str) -> str:
        """Get the appropriate style for a status string."""
        status_lower = status.lower()
        if status_lower in {"done", "success", "completed"}:
            return "green"

        if status_lower in {"failed", "error"}:
            return "red"

        return "magenta"

    def _base_get_result(self, _entity_id: str) -> T:
        """
        Fetch the final result for an operation.

        You'll need to implement this method to make a GET request to retrieve
        the final result for the operation.
        """
        raise NotImplementedError("Subclasses must implement get_result")

    async def _base_get_result_async(self, _entity_id: str) -> T:
        """
        Fetch the final result for an operation asynchronously.

        You'll need to implement this method to make a GET request to retrieve
        the final result for the operation.
        """
        raise NotImplementedError("Subclasses must implement get_result_async")
