import unittest
from typing import Annotated, Any

from pydantic import BaseModel, TypeAdapter

from tensorlake.applications import SDKUsageError
from tensorlake.applications.remote.fake_json import fake_json


# Tests that fake json generated for a type hint deserializes back to a value with the same type hint.
class FakeJSONDeserializesToSourceTypeHint(unittest.TestCase):
    def test_int(self):
        TypeAdapter(int).validate_json(fake_json(int))

    def test_list_of_str(self):
        TypeAdapter(list[str]).validate_json(fake_json(list[str]))

    def test_dict_str_to_int(self):
        TypeAdapter(dict[str, int]).validate_json(fake_json(dict[str, int]))

    def test_pydantic_model(self):
        class SampleModel(BaseModel):
            id: int
            name: str

        TypeAdapter(SampleModel).validate_json(fake_json(SampleModel))

    def test_nested_pydantic_model(self):
        class InnerModel(BaseModel):
            value: float

        class OuterModel(BaseModel):
            inner: InnerModel
            tags: list[str]

        TypeAdapter(OuterModel).validate_json(fake_json(OuterModel))

    def test_dict_of_models_or_none(self):
        class ItemModel(BaseModel):
            item_id: int
            description: str

        TypeAdapter(dict[str, ItemModel | None]).validate_json(
            fake_json(dict[str, ItemModel | None])
        )

    def test_any(self):
        TypeAdapter(Any).validate_json(fake_json(Any))

    def test_annotated_type(self):
        AnnotatedType = Annotated[int, "This is an annotated int"]
        TypeAdapter(AnnotatedType).validate_json(fake_json(AnnotatedType))

    def test_not_json_serializable_type_fails(self):
        class NonSerializable:
            pass

        with self.assertRaises(SDKUsageError) as context:
            fake_json(NonSerializable)

        self.assertTrue(
            str(context.exception).startswith(
                f"Failed to generate fake JSON for type hint {NonSerializable}, "
                "please ensure the type is JSON serializable:"
            )
        )


if __name__ == "__main__":
    unittest.main()
