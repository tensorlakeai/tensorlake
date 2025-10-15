from pydantic import BaseModel, Field


class Retries(BaseModel):
    """Retries configuration with validation.

    Retries set on an application defines default retry policy for all functions in the application.
    If a function has its own retry policy, it will override the application's retry policy.
    """

    max_retries: int = Field(default=0, description="Maximum number of retries")

    # The rest of the parameters are not implemented yet.
    # Documentation for the not implemented parameters:
    #
    # initial_delay (float): Delay in seconds before the first retry.
    # delay_multiplier (float): Multiplier applied to the delay on each retry after the first one.
    #                             A delay_multiplier of 1.0 means that the delay will always be equal to the initial_delay.
    #                             A delay_multiplier of 2.0 means that the delay will double after each retry.
    # max_delay (float): Maximum delay in seconds between retries.
    initial_delay: float = Field(
        default=1.0, description="Delay in seconds before the first retry"
    )
    max_delay: float = Field(
        default=60.0, description="Maximum delay in seconds between retries"
    )
    delay_multiplier: float = Field(
        default=2.0, description="Multiplier applied to the delay on each retry"
    )
