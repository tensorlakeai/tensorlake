from tensorlake.vendor.nanoid import generate as nanoid

from .interface.package import Package, define_package
from .registry import get_package


def get_user_defined_or_tmp_package() -> Package:
    """Returns user defined package or creates a new temporary package used until program exits.

    This function allows users to not define packages if they only need to run some code.
    """
    package: Package | None = get_package()
    if package is None:
        return define_tmp_package()
    else:
        return package


def define_tmp_package() -> Package:
    """Creates a new temporary package that is usable until program exits."""
    return define_package(name=f"temporary-{nanoid()}")
