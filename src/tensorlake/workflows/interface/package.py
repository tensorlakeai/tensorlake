from tensorlake.functions_sdk.retries import Retries
from tensorlake.vendor.nanoid import generate as nanoid

from ..registry import set_package


class Package:
    def __init__(self, name: str, version: str, retries: Retries, region: str | None):
        self._name: str = name
        self._version: str = version
        self._retries: Retries = retries
        self._region: str | None = region

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def retries(self) -> Retries:
        return self._retries

    @property
    def region(self) -> str | None:
        return self._region

    def __repr__(self) -> str:
        region = self._region if self._region else "None"
        return f"Tensorlake Package(name='{self._name}', version='{self._version}', retries={self._retries}, region={region})"


def define_package(
    name: str, retries: Retries = Retries(), region: str | None = None
) -> Package:
    # Use a unique random version. We don't provide user controlled versioning at the moment.
    version: str = nanoid()
    package = Package(name=name, version=version, retries=retries, region=region)
    set_package(package)
    return package
