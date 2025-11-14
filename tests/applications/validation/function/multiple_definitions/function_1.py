from tensorlake.applications import (
    application,
    function,
)


@application()
@function()
def function_1(_: str) -> str:
    return "function_1.py.function_1"
