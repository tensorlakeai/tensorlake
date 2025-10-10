from .future import Future


class TensorlakeCallable:
    """Base class for all callable objects in Tensorlake applications."""

    def __init__(self):
        pass

    def __call__(self, *args, **kwargs):
        """Does a blocking synchronous call of the callable object and returns the result."""
        raise NotImplementedError("Subclasses should implement this method")

    # TODO: check the feedback and voting for names before proceeding.
    def run(self, *args, **kwargs) -> Future:
        """Calls the callable object asynchronously and returns a Future."""
        raise NotImplementedError("Subclasses should implement this method")

    def run_later(self, start_delay: float, *args, **kwargs) -> Future:
        """Schedules a delayed call of the callable object and returns a Future.

        start_delay is in seconds and should not be negative.
        """
        raise NotImplementedError("Subclasses should implement this method")
