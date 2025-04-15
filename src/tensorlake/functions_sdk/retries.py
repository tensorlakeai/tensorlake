class Retries:
    def __init__(
        self,
        max_retries: int = 0,
        initial_delay: float = 1.0,
        max_delay: float = 60.0,
        delay_multiplier: float = 2.0,
    ):
        """Creates a Retries object.

        Retries set on a graph define default retry policy for all functions in the graph.
        If a function has its own retry policy, it will override the graph's retry policy.

        Args:
            max_retries (int): Maximum number of retries.
            initial_delay (float): Delay in seconds before the first retry.
            delay_multiplier (float): Multiplier applied to the delay on each retry after the first one.
                                      A delay_multiplier of 1.0 means that the delay will always be equal to the initial_delay.
                                      A delay_multiplier of 2.0 means that the delay will double after each retry.
            max_delay (float): Maximum delay in seconds between retries.
        """
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.delay_multiplier = delay_multiplier
