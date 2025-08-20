import json
import pickle
from typing import Any, List

import pydantic


class UserDataSerializer:
    """A serializer used to serialize and deserialize user data.

    The serializer must only be used for user data because it implements
    heuristics specific to user data.
    """

    def serialize(self, object: Any) -> bytes:
        """Serializes the given object into bytes."""
        raise NotImplementedError("Subclasses should implement this method.")

    def deserialize(self, data: bytes, possible_types: List[Any]) -> Any:
        """Deserializes the given bytes into an object.

        The `possible_types` parameter specify possible types of the deserialized object.
        """
        raise NotImplementedError("Subclasses should implement this method.")


class JSONUserDataSerializer(UserDataSerializer):
    """A serializer that does text serialization into JSON format.

    It serializes and deserializes basic Python types listed at
    https://docs.python.org/3/library/json.html#py-to-json-table.
    The JSON format for all the basic types is compatible with other programming languages.

    It also serializes Pydantic models. A correct type hint is required for deserialization to work.
    """

    NAME = "json"
    CONTENT_TYPE = "application/json"

    def serialize(self, object: Any) -> bytes:
        try:
            if isinstance(object, pydantic.BaseModel):
                return object.model_dump_json().encode("utf-8")
            else:
                return json.dumps(object).encode("utf-8")
        except Exception as e:
            raise ValueError(
                f"failed to serialize data with json serializer: {e}"
            ) from e

    def deserialize(self, data: bytes, possible_types: List[Any]) -> Any:
        model_classes: List[Any] = [
            t for t in possible_types if issubclass(t, pydantic.BaseModel)
        ]

        decoded_data: str = data.decode("utf-8")

        # Pydantic model deserialization heuristic.
        # Try each possible model class one by one until one succeeds,
        # otherwise, use default JSON deserialization.
        #
        # This heuristic won't work for customers who use multiple similar model classes
        # that can deserialize into each other. If this becomes a problem then we can
        # record actual class name of each object during json serialization and use it on
        # deserialization.
        for cls in model_classes:
            try:
                return cls.model_validate_json(decoded_data)
            except Exception:
                continue

        try:
            return json.loads(decoded_data)
        except Exception as e:
            raise ValueError(
                f"failed to deserialize data with json serializer: {e}"
            ) from e


class PickleUserDataSerializer(UserDataSerializer):
    """A serializer that uses binary serialization with pickle.

    The pickle format is not human-readable and can only be used in Python.
    It's compatible between Python versions (3.8+ in our case).
    Users are responsible for compatibility of data they are serializing, i.e.
    a Pandas DataFrame serialized on newer Pandas versions might not be readable
    on older versions.
    """

    NAME = "pickle"
    CONTENT_TYPE = "application/octet-stream"
    _PROTOCOL_LEVEL = 5  # Python 3.8+ only, most efficient.

    def serialize(self, object: Any) -> bytes:
        try:
            return pickle.dumps(object, protocol=self._PROTOCOL_LEVEL)
        except Exception as e:
            raise ValueError(
                f"failed to serialize data with pickle serializer: {e}"
            ) from e

    def deserialize(self, data: bytes, possible_types: List[Any]) -> Any:
        try:
            return pickle.loads(data)
        except Exception as e:
            raise ValueError(
                f"failed to deserialize data with pickle serializer: {e}"
            ) from e


def serializer_by_name(serializer_name: str) -> UserDataSerializer:
    if serializer_name == PickleUserDataSerializer.NAME:
        return PickleUserDataSerializer()
    elif serializer_name == JSONUserDataSerializer.NAME:
        return JSONUserDataSerializer()
    raise ValueError(f"Unknown serializer name: {serializer_name}")


def serializer_by_content_type(serializer_content_type: str) -> UserDataSerializer:
    if serializer_content_type == JSONUserDataSerializer.CONTENT_TYPE:
        return JSONUserDataSerializer()
    elif serializer_content_type == PickleUserDataSerializer.CONTENT_TYPE:
        return PickleUserDataSerializer()
    raise ValueError(f"Unknown serializer content type: {serializer_content_type}")
