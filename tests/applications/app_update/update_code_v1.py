import time

from tensorlake.applications import application, function


@application()
@function()
def update_code_start_func(sleep_sec: int) -> str:
    time.sleep(sleep_sec)
    return update_code_end_func()


@function()
def update_code_end_func() -> str:
    return "update_code_end_func_v1"
