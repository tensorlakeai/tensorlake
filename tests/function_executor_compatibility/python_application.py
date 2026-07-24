import json

from tensorlake.applications import (
    RETURN_WHEN,
    File,
    FunctionError,
    Future,
    RequestContext,
    RequestError,
    application,
    function,
)


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
def parity_replay_mismatch(value: int) -> int:
    return parity_double(value)
