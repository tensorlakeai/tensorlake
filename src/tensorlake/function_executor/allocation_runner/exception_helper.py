import io
import pickle

from tensorlake.applications.interface import TensorlakeError


class _RestrictedExceptionUnpickler(pickle.Unpickler):
    """Unpickler that only allows deserializing TensorlakeError subclasses."""

    def find_class(self, module: str, name: str) -> type:
        cls: type = super().find_class(module, name)
        if not issubclass(cls, TensorlakeError):
            raise pickle.UnpicklingError(
                f"Refusing to deserialize non-exception class: {module}.{name}"
            )
        return cls


def serialize_user_exception(ex: TensorlakeError) -> bytes:
    try:
        return pickle.dumps(ex)
    except Exception:
        return pickle.dumps(Exception(f"{type(ex).__name__}: {ex}"))


def deserialize_user_exception(serialized_ex: bytes) -> TensorlakeError:
    try:
        return _RestrictedExceptionUnpickler(io.BytesIO(serialized_ex)).load()
    except Exception:
        return Exception("Failed to deserialize user exception")
