import inspect
import unittest

from pydantic import BaseModel

from tensorlake.applications import application, function
from tensorlake.applications.function.application_call import (
    _coerce_payload_to_kwargs,
    _deserialize_param_value,
    _get_application_param_count,
)


class Item(BaseModel):
    name: str
    price: float


class User(BaseModel):
    username: str
    email: str


@application()
@function()
def zero_params() -> str:
    return "ok"


@application()
@function()
def one_param(item: Item) -> str:
    return item.name


@application()
@function()
def two_params(item: Item, count: int) -> str:
    return f"{item.name}: {count}"


@application()
@function()
def three_params_with_default(item: Item, count: int, label: str = "default") -> str:
    return f"{label}: {item.name}: {count}"


class TestDeserializeParamValue(unittest.TestCase):
    """Tests for _deserialize_param_value function."""

    def test_empty_type_hint_returns_value_as_is(self):
        """When type_hint is empty, return value unchanged."""
        value = {"name": "test"}
        result = _deserialize_param_value(value, inspect.Parameter.empty, "json")
        self.assertEqual(result, value)

    def test_value_already_correct_type(self):
        """When value is already the correct type, return as-is."""
        item = Item(name="Widget", price=10.0)
        result = _deserialize_param_value(item, Item, "json")
        self.assertIs(result, item)  # Same object

    def test_dict_to_pydantic_model(self):
        """Dict should be converted to Pydantic model via serializer."""
        data = {"name": "Gadget", "price": 20.0}
        result = _deserialize_param_value(data, Item, "json")
        self.assertIsInstance(result, Item)
        self.assertEqual(result.name, "Gadget")
        self.assertEqual(result.price, 20.0)

    def test_non_pydantic_dict_returns_as_is(self):
        """Dict with non-Pydantic type hint returns as-is."""
        value = {"key": "value"}
        result = _deserialize_param_value(value, dict, "json")
        self.assertEqual(result, value)

    def test_primitive_types(self):
        """Primitive types should return as-is when matching."""
        self.assertEqual(_deserialize_param_value(42, int, "json"), 42)
        self.assertEqual(_deserialize_param_value("hello", str, "json"), "hello")
        self.assertEqual(_deserialize_param_value(3.14, float, "json"), 3.14)
        self.assertEqual(_deserialize_param_value(True, bool, "json"), True)


class TestCoercePayloadToKwargs(unittest.TestCase):
    """Tests for _coerce_payload_to_kwargs function."""

    def test_all_params_provided(self):
        """All parameters provided in payload."""
        payload = {
            "item": {"name": "Test", "price": 10.0},
            "count": 5,
        }
        result = _coerce_payload_to_kwargs(two_params, payload)

        self.assertIn("item", result)
        self.assertIn("count", result)
        self.assertIsInstance(result["item"], Item)
        self.assertEqual(result["item"].name, "Test")
        self.assertEqual(result["count"], 5)

    def test_with_defaults_all_provided(self):
        """All params including those with defaults provided."""
        payload = {
            "item": {"name": "Test", "price": 10.0},
            "count": 3,
            "label": "custom",
        }
        result = _coerce_payload_to_kwargs(three_params_with_default, payload)

        self.assertEqual(result["label"], "custom")

    def test_with_defaults_only_required(self):
        """Only required params provided, defaults should be used."""
        payload = {
            "item": {"name": "Test", "price": 10.0},
            "count": 3,
        }
        result = _coerce_payload_to_kwargs(three_params_with_default, payload)

        self.assertEqual(result["label"], "default")

    def test_missing_required_param_raises_helpful_error(self):
        """Missing required parameter should raise SDKUsageError with helpful message."""
        from tensorlake.applications.interface import SDKUsageError

        payload = {
            "item": {"name": "Test", "price": 10.0},
            # Missing 'count' which is required
        }
        with self.assertRaises(SDKUsageError) as context:
            _coerce_payload_to_kwargs(two_params, payload)

        error_msg = str(context.exception)
        self.assertIn("count", error_msg)
        self.assertIn("Missing required parameter", error_msg)


class TestGetApplicationParamCount(unittest.TestCase):
    """Tests for _get_application_param_count function."""

    def test_zero_params(self):
        """Function with no params should return 0."""
        self.assertEqual(_get_application_param_count(zero_params), 0)

    def test_one_param(self):
        """Function with one param should return 1."""
        self.assertEqual(_get_application_param_count(one_param), 1)

    def test_two_params(self):
        """Function with two params should return 2."""
        self.assertEqual(_get_application_param_count(two_params), 2)

    def test_three_params_with_default(self):
        """Function with three params (one with default) should return 3."""
        self.assertEqual(_get_application_param_count(three_params_with_default), 3)


if __name__ == "__main__":
    unittest.main()
