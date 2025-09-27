import tensorlake.workflows.interface as tensorlake


# FIXME: Temporary use "pickle" serializer until root function call of the returned
# call tree inherits its output serializer from the API function.
@tensorlake.api(output_serializer="pickle")
@tensorlake.function()
def update_code_start_func(_sleep_sec: int) -> str:
    return update_code_end_func()

@tensorlake.function()
def update_code_end_func() -> str:
    return "update_code_end_func_v2"
