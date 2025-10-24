import pickle

from .function_call import FunctionCallMetadata
from .reduce_operation import ReduceOperationMetadata
from .value import ValueMetadata

_PICKLE_PROTOCOL_LEVEL = 5  # Python 3.8+ only, most efficient.


def serialize_metadata(
    metadata: ValueMetadata | FunctionCallMetadata | ReduceOperationMetadata,
) -> bytes:
    # Use pickle binary serialization format because it allows us to serialize type hints (types)
    # easily, it's also space efficient. We still need to care about compatibility between versions
    # of metadata objects used by different SDK versions. This is because theoretically customer images
    # of the same application version may use different SDK versions and because when ppl deploy their
    # application their running requests can be updated to run on the new SDK version.
    return pickle.dumps(metadata, protocol=_PICKLE_PROTOCOL_LEVEL)


def deserialize_metadata(
    data: bytes,
) -> ValueMetadata | FunctionCallMetadata | ReduceOperationMetadata:
    return pickle.loads(data)
