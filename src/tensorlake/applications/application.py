from .interface.application import Application, define_application
from .registry import get_application


def get_user_defined_or_default_application() -> Application:
    """Returns user defined application or uses default application.

    This function allows users to not define their own application
    if they are okay to use the default one.
    """
    application: Application | None = get_application()
    if application is None:
        return define_default_application()
    else:
        return application


def define_default_application() -> Application:
    """Defines default application in the current program."""
    return define_application(name="default")
