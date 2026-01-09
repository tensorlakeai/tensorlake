import inspect
import json
import pickle
from typing import Any, List, Set, Tuple, get_origin

import pydantic

from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObjectEncoding,
)

from .interface.exceptions import (
    DeserializationError,
    InternalError,
    SerializationError,
)

# API functions use serializers customizable by users because API functions
# can be called over HTTP where a serialized payload is passed.
# All non-API functions are called using Python SDK where Pickle is available.
# Pickle can serialize any Python data object and it's compatible between
# different Python versions. This allows users to not care about serialization
# format used between non-API function calls. This is why we use it for all non-API functions.
NON_API_FUNCTION_SERIALIZER_NAME: str = "pickle"


class UserDataSerializer:
    """A serializer used to serialize and deserialize user data.

    The serializer must only be used for user data because it implements
    heuristics specific to user data.
    """

    @property
    def name(self) -> str:
        """Returns the name of the serializer."""
        raise InternalError("Subclasses should implement this method.")

    @property
    def content_type(self) -> str:
        """Returns the content type of the serializer."""
        raise InternalError("Subclasses should implement this method.")

    @property
    def serialized_object_encoding(self) -> SerializedObjectEncoding:
        """Returns the serialized object encoding of the serializer."""
        raise InternalError("Subclasses should implement this method.")

    def serialize(self, object: Any) -> bytes:
        """Serializes the given object into bytes.

        Raises SerializationError on failure.
        """
        raise InternalError("Subclasses should implement this method.")

    def deserialize(
        self, data: bytearray | bytes | memoryview, possible_types: List[Any]
    ) -> Any:
        """Deserializes the given bytes into an object.

        The `possible_types` parameter specify possible types of the deserialized object.
        Raises DeserializationError on failure.
        """
        raise InternalError("Subclasses should implement this method.")


class JSONUserDataSerializer(UserDataSerializer):
    """A serializer that does text serialization into JSON format.

    It serializes and deserializes basic Python types listed at
    https://docs.python.org/3/library/json.html#py-to-json-table.
    The JSON format for all the basic types is compatible with other programming languages.

    It also serializes Pydantic models. A correct type hint is required for deserialization to work.
    """

    NAME = "json"
    CONTENT_TYPE = "application/json; charset=UTF-8"

    @property
    def name(self) -> str:
        return self.NAME

    @property
    def content_type(self) -> str:
        return self.CONTENT_TYPE

    @property
    def serialized_object_encoding(self) -> SerializedObjectEncoding:
        return SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON

    def serialize(self, object: Any) -> bytes:
        try:
            # FIXME: This heuristic doesn't cover natural cases like
            # List[BaseModel], Dict[str, BaseModel], etc.
            # These are easy to serialize to json too.
            if isinstance(object, pydantic.BaseModel):
                return object.model_dump_json().encode("utf-8")
            else:
                if isinstance(object, set):
                    # json.dumps doesn't support sets natively.
                    object = list(object)
                return json.dumps(object).encode("utf-8")
        except Exception as e:
            raise SerializationError(
                f"Failed to serialize data with json serializer: {e}"
            ) from e

    def deserialize(
        self, data: bytearray | bytes | memoryview, possible_types: List[Any]
    ) -> Any:
        model_classes: List[Any] = [
            t
            for t in possible_types
            if inspect.isclass(t) and issubclass(t, pydantic.BaseModel)
        ]

        # JSON objects are typically small so it's ok to convert memoryview to bytes here.
        decoded_data: str = (
            data.tobytes().decode("utf-8")
            if isinstance(data, memoryview)
            else data.decode("utf-8")
        )

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
            deserialized_value: Any = json.loads(decoded_data)
        except Exception as e:
            raise DeserializationError(
                f"Failed to deserialize data with json serializer: {e}"
            ) from e

        if isinstance(deserialized_value, list):
            # Tuple is serialized as array by json.dumps.
            # Handles type hints: Tuple[T], tuple[T], tuple.
            is_tuple: bool = any(
                get_origin(t) is Tuple or get_origin(t) is tuple or t is tuple
                for t in possible_types
            )
            # Set is serialized as array by json.dumps.
            # Handles type hints: Set[T], set[T], set.
            is_set: bool = any(
                get_origin(t) is Set or get_origin(t) is set or t is set
                for t in possible_types
            )
            if is_tuple:
                deserialized_value = tuple(deserialized_value)
            elif is_set:
                deserialized_value = set(deserialized_value)

        return deserialized_value


class PickleUserDataSerializer(UserDataSerializer):
    """A serializer that uses binary serialization with pickle.

    The pickle format is not human-readable and can only be used in Python.
    It's compatible between Python versions (3.8+ in our case).
    Users are responsible for compatibility of data they are serializing, i.e.
    a Pandas DataFrame serialized on newer Pandas versions might not be readable
    on older versions.
    """

    NAME = "pickle"
    CONTENT_TYPE = "application/python-pickle"
    _PROTOCOL_LEVEL = 5  # Python 3.8+ only, most efficient.

    @property
    def name(self) -> str:
        return self.NAME

    @property
    def content_type(self) -> str:
        return self.CONTENT_TYPE

    @property
    def serialized_object_encoding(self) -> SerializedObjectEncoding:
        return SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE

    def serialize(self, object: Any) -> bytes:
        try:
            return pickle.dumps(object, protocol=self._PROTOCOL_LEVEL)
        except Exception as e:
            raise SerializationError(
                f"Failed to serialize data with pickle serializer: {e}"
            ) from e

    def deserialize(
        self, data: bytearray | bytes | memoryview, possible_types: List[Any]
    ) -> Any:
        try:
            return pickle.loads(data)
        except Exception as e:
            raise DeserializationError(
                f"Failed to deserialize data with pickle serializer: {e}"
            ) from e


def serializer_by_name(serializer_name: str) -> UserDataSerializer:
    """Returns the UserDataSerializer instance for the given serializer name.

    The caller must validate the serializer name beforehand.
    Raises InternalError if the serializer name is unknown.
    """
    if serializer_name == PickleUserDataSerializer.NAME:
        return PickleUserDataSerializer()
    elif serializer_name == JSONUserDataSerializer.NAME:
        return JSONUserDataSerializer()
    # We're validating application serializers on app deployment so this should never happen.
    raise InternalError(f"Unknown serializer name: {serializer_name}")
