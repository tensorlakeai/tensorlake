from typing import Type, TypeVar

from pydantic import BaseModel, TypeAdapter

from tensorlake.applications.interface import SDKUsageError
from tensorlake.vendor.polyfactory.factories.pydantic_factory import ModelFactory

_FAKE_DATA_TYPE_HINT = TypeVar("_FAKE_DATA_TYPE_HINT")


def fake_json(type_hint: Type[_FAKE_DATA_TYPE_HINT]) -> str:
    """Generates a Pydantic model instance populated with fake data for the given type hint.

    Raises SDKUsageError if the supplied type hint is not JSON serializable.
    """

    try:
        # Dynamically create a wrapper model.
        class Wrapper(BaseModel):
            value: type_hint  # type: ignore

        # Dynamically create a factory
        class Factory(ModelFactory[Wrapper]):
            __model__ = Wrapper
            # Ensures deterministic fake data generation,
            # including same choice of type in Union fields and all other values.
            __random_seed__ = 10
            # Defaults to True. Set to False to prevent 'None' in Optional/Union fields.
            __allow_none_optionals__ = False

        fake_value: _FAKE_DATA_TYPE_HINT = Factory.build().value
        adapter: TypeAdapter[_FAKE_DATA_TYPE_HINT] = TypeAdapter(type_hint)
        return adapter.dump_json(fake_value).decode("utf-8")
    except Exception as e:
        raise SDKUsageError(
            f"Failed to generate fake JSON for type hint {type_hint}, please ensure the type is JSON serializable: {e}"
        ) from e
