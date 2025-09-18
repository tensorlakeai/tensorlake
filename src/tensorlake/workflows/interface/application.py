from typing import Dict

from tensorlake.vendor.nanoid import generate as nanoid

from ..registry import set_application
from .function import Function
from .retries import Retries


class Application:
    def __init__(
        self,
        name: str,
        description: str,
        default_api: Function | None,
        tags: Dict[str, str],
        version: str,
        retries: Retries,
        region: str | None,
    ):
        self._name: str = name
        self._description: str = description
        self._default_api: Function | None = default_api
        self._tags: Dict[str, str] = tags
        self._version: str = version
        self._retries: Retries = retries
        self._region: str | None = region

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return self._description

    @property
    def default_api_function(self) -> Function | None:
        return self._default_api

    @property
    def tags(self) -> Dict[str, str]:
        return self._tags

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
        return f"Tensorlake Application(name='{self._name}', version='{self._version}', retries={self._retries}, region={region})"


def define_application(
    name: str,
    description: str = "",
    default_api: Function | None = None,
    tags: Dict[str, str] = {},
    retries: Retries = Retries(),
    region: str | None = None,
) -> Application:
    # Use a unique random version. We don't provide user controlled versioning at the moment.
    version: str = nanoid()
    application = Application(
        name=name,
        description=description,
        default_api=default_api,
        tags=tags.copy(),
        version=version,
        retries=retries,
        region=region,
    )
    set_application(application)
    return application
