import time

import tensorlake.applications


@tensorlake.applications.api()
@tensorlake.applications.function()
def update_code_start_func(sleep_sec: int) -> str:
    time.sleep(sleep_sec)
    return update_code_end_func()


@tensorlake.applications.function()
def update_code_end_func() -> str:
    return "update_code_end_func_v1"
