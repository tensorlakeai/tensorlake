import multiprocessing


def setup_multiprocessing() -> None:
    """Setup multiprocessing settings for TensorLake applications.

    Must be called before running any customer code that might use multiprocessing.
    Can be called multiple times; subsequent calls have no effect.
    """
    # Set "spawn" method because grpc Server doesn't support forking. exec is required.
    # "spawn" is supported by all OSes: Windows, Linux, MacOS.
    # "spawn" is also required to avoid issues with inherited resources in child processes.
    # i.e. open file descriptors, network connections, context variables, etc.
    multiprocessing.set_start_method(method="spawn", force=True)
