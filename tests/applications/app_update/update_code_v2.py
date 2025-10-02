from tensorlake.applications import function


@function()
def update_code_end_func() -> str:
    return "update_code_end_func_v2"
