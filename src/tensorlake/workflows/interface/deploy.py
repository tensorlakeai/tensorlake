from ..package import get_user_defined_or_tmp_package
from .package import Package


def deploy() -> None:
    """Deploys all the Tensorlake Functions to Tensorlake Cloud."""
    # TODO: Validate the graph.
    # TODO: Deploy graph.
    package: Package = get_user_defined_or_tmp_package()
    print("Deploying package:", package)
    raise NotImplementedError("Deploy is not implemented yet.")
