class Retries:
    def __init__(
        self,
        max_retries: int = 0,
    ):
        """Creates a Retries object.

        Retries set on an application defines default retry policy for all functions in the application.
        If a function has its own retry policy, it will override the application's retry policy.

        Args:
            max_retries (int): Maximum number of retries.
        """
        self.max_retries = max_retries
        # The rest of the parameters are not implemented yet.
        # Documentation for the not implemented parameters:
        #
        # initial_delay (float): Delay in seconds before the first retry.
        # delay_multiplier (float): Multiplier applied to the delay on each retry after the first one.
        #                             A delay_multiplier of 1.0 means that the delay will always be equal to the initial_delay.
        #                             A delay_multiplier of 2.0 means that the delay will double after each retry.
        # max_delay (float): Maximum delay in seconds between retries.
        self.initial_delay = 1.0
        self.max_delay = 60.0
        self.delay_multiplier = 2.0
