from typing import Any

from ..interface.application import Application
from ..interface.function import Function
from ..interface.request import Request


class RemoteRunner:
    def __init__(self, application: Application, api: Function, payload: Any):
        self._application: Application = application
        self._api: Function = api
        self._payload: Any = payload

    def run(self) -> Request:
        raise NotImplementedError("RemoteRunner is not implemented yet.")
