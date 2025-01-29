from first_p_dep import return_x

from tensorlake.functions_sdk.functions import tensorlake_function


@tensorlake_function()
def extractor_a(a: int) -> int:
    """
    Do stuff.
    """
    print("Running executor")
    return return_x(x=a)


@tensorlake_function()
def extractor_c(s: str) -> str:
    """
    Do nothing, just return.
    """
    return "this is a return from extractor_c"
