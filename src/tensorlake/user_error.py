class InvocationError(Exception):
    """An invocation cannot complete; the task should not be retried.

    This exception is raised by user code executing within a workflow
    graph in order to indicate a serious problem with a particular
    graph invocation -- typically things like invalid arguments
    supplied to the workflow.
    """

    def __init__(self, message: str):
        super().__init__(message)

    @property
    def message(self) -> str:
        return self.args[0]
