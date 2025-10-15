class StopFunctionRun(BaseException):
    """Exception to raise in a function run to stop it immediately.

    Expected to be caught by local runner LocalFunctionRun thread and silently dropped.
    Inherited from BaseException so that it is not caught by most exception handlers
    i.e. in user code. If caught by user code then stopping LocalFunctionRun thread
    will not happen quickly.
    """

    def __init__(self):
        super().__init__("Function run stopped")
