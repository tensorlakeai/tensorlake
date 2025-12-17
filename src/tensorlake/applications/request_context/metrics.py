from typing import Any

from tensorlake.applications.cloud_events import print_cloud_event


def print_counter_incremented_event(
    request_id: str,
    function_name: str,
    counter_name: str,
    counter_value: int | float,
    local_mode: bool,
) -> None:
    """Prints a counter incremented event to stdout.

    Uses human-readable format in local mode, and Cloud Events format otherwise (remote/FE mode).
    Doesn't raise any exceptions.
    """
    event: dict[str, Any] = {
        "request_id": request_id,
        "function_name": function_name,
        "counter_name": counter_name,
        "counter_inc": counter_value,  # this not current value but value to increment by
    }

    if local_mode:
        print(f"Counter Incremented: {event}", flush=True)
    else:
        try:
            print_cloud_event(
                event=event,
                type="ai.tensorlake.metric.counter.inc",
                source="/tensorlake/applications/metrics",
            )
        except Exception:
            print(
                f"Failed to print counter incremented cloud event: ",
                counter_name,
                counter_value,
                flush=True,
            )


def print_timer_recorded_event(
    request_id: str,
    function_name: str,
    timer_name: str,
    timer_value: int | float,
    local_mode: bool,
) -> None:
    """Prints a timer recorded event to stdout.

    Uses human-readable format in local mode, and Cloud Events format otherwise (remote/FE mode).
    Doesn't raise any exceptions.
    """
    event: dict[str, Any] = {
        "request_id": request_id,
        "function_name": function_name,
        "timer_name": timer_name,
        "timer_value": timer_value,
    }

    if local_mode:
        print(f"Timer Recorded: {event}", flush=True)
    else:
        try:
            print_cloud_event(
                event=event,
                type="ai.tensorlake.metric.timer",
                source="/tensorlake/applications/metrics",
            )
        except Exception:
            print(
                f"Failed to print timer recorded cloud event: ",
                timer_name,
                timer_value,
                flush=True,
            )
