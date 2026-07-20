from tensorlake.applications import (
    File,
    FunctionError,
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


@application()
@function()
def parity_value(value: int) -> dict[str, int]:
    return {"value": value}


@application()
@function()
def parity_child(value: int) -> int:
    return parity_double(value)


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
def parity_handled_creation_failure(value: int) -> str:
    try:
        parity_failing_child(value)
    except FunctionError:
        return "caught:creation_error"
    return "unexpected:success"


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
