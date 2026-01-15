import pickle
from typing import Any

import pydantic

from tensorlake.function_executor.proto.function_executor_pb2 import (
    SerializedObjectEncoding,
)

from .function.type_hints import is_pydantic_type_hint
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

    def serialize(self, object: Any, type_hints: list[Any]) -> bytes:
        """Serializes the given object into bytes.

        The `type_hints` parameter specifies possible types of the serialized object.
        type(object) is not the same as type_hints[0], i.e.
        type(object) is list, type_hints[0] is List[int].

        Raises SerializationError on failure.
        """
        raise InternalError("Subclasses should implement this method.")

    def deserialize(
        self, data: bytearray | bytes | memoryview, type_hints: list[Any]
    ) -> Any:
        """Deserializes the given bytes into an object.

        The `type_hints` parameter specifies possible types of the deserialized object.
        type(object) is not the same as type_hints[0], i.e.
        type(object) is list, type_hints[0] is List[int].

        Raises DeserializationError on failure.
        """
        raise InternalError("Subclasses should implement this method.")


class JSONUserDataSerializer(UserDataSerializer):
    """A serializer that does text serialization into JSON format.

    It serializes Pydantic models and built-in Python types supported by Pydantic.
    The models and built-in types must be JSON serializable by Pydantic.
    This approach supports much much more use cases than standard json module.
    i.e. it support model fields inside built-in types like dict[str, ModelClass],
    it converts json lists into sets if deserializing into set[...],
    it converts json string object keys into int keys if deserializing into dict[int, ...], etc.
    See more at https://docs.pydantic.dev/latest/concepts/serialization/#json-mode.
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

    def serialize(self, object: Any, type_hints: list[Any]) -> bytes:
        if isinstance(object, pydantic.BaseModel):
            try:
                return object.model_dump_json().encode("utf-8")
            except Exception as e:
                raise SerializationError(
                    f"Failed to serialize Pydantic model {object} to json: {e}"
                )

        # Serialization heuristic: try each possible type hint one by one until one succeeds.
        # This is similar to how FastAPI works, see
        # https://fastapi.tiangolo.com/tutorial/extra-models/#union-or-anyof.
        last_exception: SerializationError | None = None
        for type_hint in type_hints:
            if is_pydantic_type_hint(type_hint):
                continue  # handled above
            try:
                return pydantic.TypeAdapter(type_hint).dump_json(
                    object, warnings="error"
                )
            except Exception as e:
                last_exception = SerializationError(
                    f"Failed to serialize {object} as {type_hint} to json: {e}"
                )
                continue

        if last_exception is None:
            # Only create the default exception when needed to avoid rendering potentially large object as str.
            last_exception = SerializationError(
                f"Failed to serialize {object} to json: the provided type hints are "
                "Pydantic models but the object is not a Pydantic model."
            )

        raise last_exception

    def deserialize(
        self, data: bytearray | bytes | memoryview, type_hints: list[Any]
    ) -> Any:
        if isinstance(data, memoryview):
            # Pydantic only supports bytes or bytearray.
            data: bytes | bytearray = data.tobytes()

        # Deserialization heuristic: try each possible type hint one by one until one succeeds.
        # This is similar to how FastAPI works, see
        # https://fastapi.tiangolo.com/tutorial/extra-models/#union-or-anyof.
        last_exception: DeserializationError = DeserializationError(
            "Failed to deserialize object from json: no type hints were provided."
        )
        for type_hint in type_hints:
            try:
                return self._try_deserialize(data, type_hint)
            except DeserializationError as e:
                last_exception = e
                continue

        raise last_exception

    def _try_deserialize(self, data: bytes | bytearray, type_hint: Any) -> Any:
        """Tries to deserialize the given data into the given type hint.

        Raises DeserializationError on failure.
        """
        try:
            if is_pydantic_type_hint(type_hint):
                return type_hint.model_validate_json(data)
            else:
                return pydantic.TypeAdapter(type_hint).validate_json(data)
        except Exception as e:
            raise DeserializationError(
                f"Failed to deserialize data with json serializer: {e}"
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

    def serialize(self, object: Any, type_hints: list[Any]) -> bytes:
        try:
            return pickle.dumps(object, protocol=self._PROTOCOL_LEVEL)
        except Exception as e:
            raise SerializationError(
                f"Failed to serialize data with pickle serializer: {e}"
            ) from e

    def deserialize(
        self, data: bytearray | bytes | memoryview, type_hints: list[Any]
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
