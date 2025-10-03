import time

from tensorlake.applications import application, function


@application()
@function()
def code_update_start_func(sleep_sec: int) -> str:
    time.sleep(sleep_sec)
    return code_update_end_func()


@function()
def code_update_end_func() -> str:
    return "END_FUNC_CODE_VERSION_PLACEHOLDER"
