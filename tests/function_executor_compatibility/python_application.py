import hashlib
import json
import threading
import time

from tensorlake.applications import (
    RETURN_WHEN,
    File,
    FunctionError,
    Future,
    HttpBody,
    RequestContext,
    RequestError,
    SDKUsageError,
    TimeoutError,
    application,
    function,
)

_PREINITIALIZED_TICK = threading.Event()


def _emit_preinitialized_ticks() -> None:
    while True:
        time.sleep(0.025)
        _PREINITIALIZED_TICK.set()


threading.Thread(target=_emit_preinitialized_ticks, daemon=True).start()


@function()
def parity_double(value: int) -> int:
    return value * 2


@function()
def parity_add(accumulator: int, value: int) -> int:
    return accumulator + value


@function()
def parity_failing_child(value: int) -> int:
    raise RuntimeError(f"child failed for {value}")


@function()
def parity_request_failing_child(value: int) -> int:
    raise RequestError(f"child request failed for {value}")


@function()
def parity_identity_file(value: File) -> File:
    return value


@application()
@function()
def parity_value(value: int) -> dict[str, int]:
    return {"value": value}


@application()
@function()
def parity_multipart(left: int, right: int) -> int:
    return left * right


@application()
@function()
def parity_child(value: int) -> int:
    return parity_double(value)


@application()
@function()
def parity_wait_first_failure_after_success(value: int) -> dict[str, int]:
    completed = parity_double.future(value).run()
    completed.result()
    pending = parity_double.future(value + 1)
    done, not_done = Future.wait(
        [completed, pending],
        return_when=RETURN_WHEN.FIRST_FAILURE,
    )
    return {"done": len(done), "not_done": len(not_done)}


@application()
@function()
def parity_wait_first_failure_after_success_and_failure(
    value: int,
) -> dict[str, int]:
    completed = parity_double.future(value).run()
    completed.result()
    failing = parity_failing_child.future(value + 1)
    done, not_done = Future.wait(
        [completed, failing],
        return_when=RETURN_WHEN.FIRST_FAILURE,
    )
    return {"done": len(done), "not_done": len(not_done)}


@application()
@function()
def parity_wait_causal_replay(value: int) -> dict[str, object]:
    first = parity_double.future(value).run()
    second = parity_double.future(value + 1).run()
    done, not_done = Future.wait(
        [first, second],
        return_when=RETURN_WHEN.FIRST_COMPLETED,
    )
    _PREINITIALIZED_TICK.clear()
    _PREINITIALIZED_TICK.wait()
    marker = parity_double(value + 2)
    return {
        "done": len(done),
        "not_done": len(not_done),
        "marker": marker,
        "results": [first.result(), second.result()],
    }


@application()
@function()
def parity_wait_batched_results(value: int) -> dict[str, object]:
    first = parity_double.future(value).run()
    second = parity_double.future(value + 1).run()
    done, not_done = Future.wait(
        [first, second],
        return_when=RETURN_WHEN.FIRST_COMPLETED,
    )
    return {
        "done": len(done),
        "not_done": len(not_done),
        "results": [first.result(), second.result()],
    }


@application()
@function()
def parity_map(value: int) -> list[int]:
    return parity_double.map([value, value + 1, value + 2])


@application()
@function()
def parity_reduce(value: int) -> int:
    return parity_add.reduce([value, value + 1, value + 2], 10)


@application()
@function()
def parity_reduce_large(value: int) -> int:
    return parity_add.reduce(list(range(1, 514)), value)


@application()
@function()
def parity_reduce_no_initial(value: int) -> int:
    return parity_add.reduce([value, value + 1, value + 2])


@application()
@function()
def parity_map_reduce(value: int) -> int:
    mapped = parity_double.map([value, value + 1, value + 2])
    return parity_add.reduce(mapped, 0)


@application()
@function()
def parity_tail_call(value: int) -> int:
    return parity_double.future(value)


@application()
@function()
def parity_handled_child_failure(value: int) -> str:
    try:
        parity_failing_child(value)
    except FunctionError:
        return "caught:function_error"
    return "unexpected:success"


@application()
@function()
def parity_handled_child_request_error(value: int) -> str:
    try:
        parity_request_failing_child(value)
    except RequestError:
        return "caught:request_error"
    return "unexpected:success"


@application()
@function()
def parity_handled_child_timeout(value: int) -> str:
    try:
        parity_double(value)
    except TimeoutError:
        return "caught:timeout"
    return "unexpected:success"


@application()
@function()
def parity_handled_creation_failure(value: int) -> str:
    try:
        parity_failing_child(value)
    except FunctionError:
        return "caught:creation_error"
    return "unexpected:success"


@application()
@function()
def parity_watcher_creation_failure(value: int) -> int:
    return parity_double(value)


@application()
@function()
def parity_request_error(value: int) -> int:
    raise RequestError(f"invalid value: {value}")


@application()
@function()
def parity_function_error(value: int) -> int:
    raise RuntimeError(f"function failed for {value}")


@application()
@function()
def parity_file(value: int) -> File:
    return File(f"parity-file-{value}".encode(), "text/plain")


@application()
@function()
def parity_json_file(value: int) -> dict:
    content = json.dumps({"value": value}, separators=(",", ":")).encode()
    result = parity_identity_file(File(content, "application/json"))
    return {
        "content": result.content.decode(),
        "content_type": result.content_type,
        "is_file": isinstance(result, File),
    }


@application()
@function()
def parity_http_body(body: HttpBody) -> dict:
    headers = RequestContext.get().headers
    return {
        "content_hex": body.content.hex(),
        "content_type": body.content_type,
        "header": headers.get("x-tensorlake-test"),
        "header_values": headers.getlist("x-tensorlake-test"),
        "is_http_body": isinstance(body, HttpBody),
        "json": body.json(),
        "text": body.text(),
    }


@application()
@function()
def parity_state(value: int) -> dict:
    context = RequestContext.get()
    missing = context.state.get("missing", {"value": -1})
    context.state.set("answer", {"value": value})
    stored = context.state.get("answer")
    context.progress.update(
        2,
        3,
        message="parity progress",
        attributes={"runtime": "shared-harness"},
    )
    return {
        "missing": missing,
        "request_id": context.request_id,
        "stored": stored,
    }


@application()
@function()
def parity_progress_validation(value: int) -> dict[str, int]:
    progress = RequestContext.get().progress
    invalid_updates = [
        {"current": -1, "total": 1},
        {"current": 1, "total": float("inf")},
        {"current": 1, "total": 1, "message": value},
        {"current": 1, "total": 1, "attributes": ["invalid"]},
        {"current": 1, "total": 1, "attributes": time.gmtime()},
    ]
    rejected = 0
    for arguments in invalid_updates:
        try:
            progress.update(**arguments)
        except SDKUsageError:
            rejected += 1
    return {"rejected": rejected}


@application()
@function()
def parity_context_validation(value: int) -> dict[str, int]:
    context = RequestContext.get()
    invalid_operations = [
        lambda: context.state.get(value),
        lambda: context.state.set(value, "invalid"),
        lambda: context.metrics.counter(value),
        lambda: context.metrics.counter("counter", 1.5),
        lambda: context.metrics.counter("counter", True),
        lambda: context.metrics.timer(value, 1),
        lambda: context.metrics.timer("timer", "invalid"),
        lambda: context.metrics.timer("timer", True),
        lambda: context.metrics.timer("timer", float("inf")),
    ]
    rejected = 0
    for operation in invalid_operations:
        try:
            operation()
        except SDKUsageError:
            rejected += 1
    return {"rejected": rejected}


@application()
@function()
def parity_replay_mismatch(value: int) -> int:
    return parity_double(value)


@application()
@function()
def parity_http_envelope(value: dict) -> dict:
    headers = RequestContext.get().headers
    return {
        "header": headers.get("x-public-invocation"),
        "value": value,
    }


@application()
@function()
def parity_http_envelope_default(name: str = "world") -> str:
    return f"Hello, {name}!"


@application()
@function()
def parity_file_input(file: File) -> dict:
    return {
        "content_hex": file.content.hex(),
        "content_type": file.content_type,
        "is_file": isinstance(file, File),
    }


@application()
@function()
def parity_multipart_http_body(body: HttpBody, metadata: dict) -> dict:
    return {
        "body_hex": body.content.hex(),
        "body_type": body.content_type,
        "metadata": metadata,
    }


@application()
@function()
def parity_empty_http_body(body: HttpBody) -> dict:
    return {
        "content_hex": body.content.hex(),
        "content_type": body.content_type,
    }


@application()
@function()
def parity_malformed_json(value: int) -> int:
    return value


@application()
@function()
def parity_chunked_http_body(body: HttpBody) -> dict:
    return {
        "sha256": hashlib.sha256(body.content).hexdigest(),
        "size": len(body.content),
    }


@application()
@function()
def parity_wait_all_completed(value: int) -> dict[str, int]:
    successful = parity_double.future(value)
    failing = parity_failing_child.future(value + 1)
    done, not_done = Future.wait(
        [successful, failing],
        return_when=RETURN_WHEN.ALL_COMPLETED,
    )
    return {
        "done": len(done),
        "failures": sum(future.exception is not None for future in done),
        "not_done": len(not_done),
    }


@application()
@function()
def parity_wait_timeout(value: int) -> dict[str, int]:
    pending = parity_double.future(value)
    done, not_done = Future.wait(
        [pending],
        timeout=0.05,
        return_when=RETURN_WHEN.ALL_COMPLETED,
    )
    return {
        "done": len(done),
        "failures": sum(future.exception is not None for future in done),
        "not_done": len(not_done),
    }


@application()
@function()
def parity_run_later(value: int) -> int:
    return parity_double.future(value).run_later(0.05).result()


@application()
@function()
def parity_detached_future(value: int) -> str:
    parity_double.future(value).run()
    return "started"


@application()
@function()
def parity_future_reuse(value: int) -> dict[str, int]:
    future = parity_double.future(value)
    first = future.result()
    second = future.result()
    return {"first": first, "second": second}


@application()
@function()
def parity_map_empty(value: int) -> list[int]:
    return parity_double.map([])


@application()
@function()
def parity_reduce_empty_initial(value: int) -> int:
    return parity_add.reduce([], value)


@application()
@function()
def parity_map_failure(value: int) -> list[int]:
    return parity_failing_child.map([value, value + 1, value + 2])


@application()
@function()
def parity_reduce_failure(value: int) -> int:
    return parity_add.reduce([value, value + 1], 0)


@application()
@function()
def parity_unhandled_child_failure(value: int) -> int:
    return parity_failing_child(value)


@application()
@function()
def parity_unhandled_child_request_error(value: int) -> int:
    return parity_request_failing_child(value)


@application()
@function()
def parity_context_events(value: int) -> dict:
    context = RequestContext.get()
    context.state.set("version", {"value": 1})
    context.state.set("version", {"value": value})
    stored = context.state.get("version")
    context.metrics.counter("processed_items", value)
    context.metrics.timer("processing_seconds", 1.25)
    context.progress.update(
        1,
        2,
        message="context halfway",
        attributes={"phase": "half"},
    )
    context.progress.update(
        2,
        2,
        message="context complete",
        attributes={"phase": "done"},
    )
    return {"stored": stored}


@application()
@function()
def parity_state_failure(value: int) -> int:
    RequestContext.get().state.get("unavailable")
    return value
