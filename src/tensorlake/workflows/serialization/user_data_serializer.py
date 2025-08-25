import json
from typing import Any

import cloudpickle

from ..interface.request_context import RequestContextPlaceholder


class UserDataSerializer:
    """A serializer used to serialize and deserialize user data.

    Must be used for all user data because it contains heuristics for different types of objects.
    So it should be invoked directly on these objects (e.g. not as a part of a higher level structure field).
    Must not be used for internal non-user data.
    """

    def serialize(self, object: Any) -> bytes:
        raise NotImplementedError("Subclasses should implement this method.")

    def deserialize(self, data: bytes) -> Any:
        raise NotImplementedError("Subclasses should implement this method.")


class JSONUserDataSerializer(UserDataSerializer):
    NAME = "json"
    CONTENT_TYPE = "application/json"
    # A special string that indicates serialized request context placeholder.
    # This workaround is required because we can't store a header in the serialized data
    # because we can't alter user supplied data.
    _REQUEST_CONTEXT_PLACEHOLDER_SPECIAL_STRING = (
        "TENSORLAKE_REQUEST_CONTEXT_PLACEHOLDER"
    )

    def serialize(self, object: Any) -> bytes:
        try:
            if isinstance(object, RequestContextPlaceholder):
                return self._REQUEST_CONTEXT_PLACEHOLDER_SPECIAL_STRING.encode("utf-8")
            else:
                return json.dumps(object).encode("utf-8")
        except Exception as e:
            raise ValueError(f"failed to serialize data with json: {e}") from e

    def deserialize(self, data: bytes) -> Any:
        try:
            data_str: str = data.decode("utf-8")
            if data_str == self._REQUEST_CONTEXT_PLACEHOLDER_SPECIAL_STRING:
                return RequestContextPlaceholder()
            else:
                return json.loads(data_str)
        except Exception as e:
            raise ValueError(f"failed to deserialize data with json: {e}") from e


class CloudPickleUserDataSerializer(UserDataSerializer):
    NAME = "cloudpickle"
    CONTENT_TYPE = "application/octet-stream"

    def serialize(self, object: Any) -> bytes:
        return cloudpickle.dumps(object)

    def deserialize(self, data: bytes) -> Any:
        return cloudpickle.loads(data)


def serializer_by_name(serializer_name: str) -> UserDataSerializer:
    if serializer_name == CloudPickleUserDataSerializer.NAME:
        return CloudPickleUserDataSerializer()
    elif serializer_name == JSONUserDataSerializer.NAME:
        return JSONUserDataSerializer()
    raise ValueError(f"Unknown serializer name: {serializer_name}")


def serializer_by_content_type(serializer_content_type: str) -> UserDataSerializer:
    if serializer_content_type == JSONUserDataSerializer.CONTENT_TYPE:
        return JSONUserDataSerializer()
    elif serializer_content_type == CloudPickleUserDataSerializer.CONTENT_TYPE:
        return CloudPickleUserDataSerializer()
    raise ValueError(f"Unknown serializer content type: {serializer_content_type}")
