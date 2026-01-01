import unittest
from typing import Any, Dict

import parameterized
from pydantic import BaseModel

from tensorlake.applications import Request, application, cls, function
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.validation import validate_loaded_applications


class Item(BaseModel):
    name: str
    description: str = ""
    price: float
    tax: float = 0.0


class User(BaseModel):
    username: str
    full_name: str


@application()
@function()
def zero_params() -> str:
    """Application with no parameters."""
    return "OK"


@application()
@function()
def single_param(item: Item) -> Dict[str, Any]:
    """Application with single parameter (backward compatible)."""
    return {"item_name": item.name, "total_price": item.price + item.tax}


@application()
@function()
def multi_params(item: Item, user: dict, importance: int) -> Dict[str, Any]:
    """Application with multiple parameters (JSON serializer, dict user)."""
    return {
        "order_summary": f"Order for {user.get('full_name', 'Unknown')}",
        "item_name": item.name,
        "importance_level": importance,
        "total_price": item.price + item.tax,
    }


@application()
@function()
def multi_params_with_defaults(
    name: str, greeting: str = "Hello", punctuation: str = "!"
) -> str:
    """Application with multiple parameters including defaults."""
    return f"{greeting}, {name}{punctuation}"


@application(input_deserializer="pickle")
@function()
def multi_params_pickle(item: Item, user: User, importance: int) -> Dict[str, Any]:
    """Application with multiple parameters using pickle serializer."""
    return {
        "order_summary": f"Order for {user.full_name}",
        "item_name": item.name,
        "importance_level": importance,
        "total_price": item.price + item.tax,
    }


@cls()
class ZeroParamsClass:
    def __init__(self):
        self.value = "initialized"

    @application()
    @function()
    def run(self) -> str:
        """Class method with no parameters (excluding self)."""
        return f"Class value: {self.value}"


@cls()
class MultiParamsClass:
    def __init__(self):
        self.multiplier = 2

    @application()
    @function()
    def run(self, item: Item, quantity: int = 1) -> Dict[str, Any]:
        """Class method with multiple parameters including defaults."""
        return {
            "item_name": item.name,
            "quantity": quantity,
            "total_price": (item.price + item.tax) * quantity * self.multiplier,
        }


class TestApplicationCalling(unittest.TestCase):
    """Integration tests for the application calling interface."""

    def test_all_applications_pass_validation(self):
        """All application functions defined in this file should pass validation."""
        messages = validate_loaded_applications()
        self.assertEqual(messages, [], f"Validation errors: {messages}")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_zero_params_empty_dict(self, _: str, is_remote: bool):
        """Zero-param function accepts empty dict payload."""
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(zero_params, {}, remote=is_remote)
        self.assertEqual(request.output(), "OK")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_zero_params_none(self, _: str, is_remote: bool):
        """Zero-param function accepts None payload."""
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(zero_params, None, remote=is_remote)
        self.assertEqual(request.output(), "OK")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_single_param_dict_payload(self, _: str, is_remote: bool):
        """Single-param function with dict payload (converted to Pydantic)."""
        if is_remote:
            deploy_applications(__file__)
        payload = {"name": "Widget", "price": 100.0, "tax": 10.0}
        request: Request = run_application(single_param, payload, remote=is_remote)
        output = request.output()
        self.assertEqual(output["item_name"], "Widget")
        self.assertEqual(output["total_price"], 110.0)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_single_param_pydantic_payload(self, _: str, is_remote: bool):
        """Single-param function with Pydantic instance payload."""
        if is_remote:
            deploy_applications(__file__)
        item = Item(name="Gadget", price=200.0, tax=20.0)
        request: Request = run_application(single_param, item, remote=is_remote)
        output = request.output()
        self.assertEqual(output["item_name"], "Gadget")
        self.assertEqual(output["total_price"], 220.0)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_multi_params_all_provided(self, _: str, is_remote: bool):
        """Multi-param function with all parameters provided in dict payload."""
        if is_remote:
            deploy_applications(__file__)
        payload = {
            "item": {"name": "Foo", "price": 42.0, "tax": 3.2},
            "user": {"username": "dave", "full_name": "Dave Grohl"},
            "importance": 5,
        }
        request: Request = run_application(multi_params, payload, remote=is_remote)
        output = request.output()
        self.assertEqual(output["order_summary"], "Order for Dave Grohl")
        self.assertEqual(output["item_name"], "Foo")
        self.assertEqual(output["importance_level"], 5)
        self.assertEqual(output["total_price"], 45.2)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_defaults_all_provided(self, _: str, is_remote: bool):
        """Multi-param function with all params including defaults provided."""
        if is_remote:
            deploy_applications(__file__)
        payload = {"name": "World", "greeting": "Hi", "punctuation": "?"}
        request: Request = run_application(
            multi_params_with_defaults, payload, remote=is_remote
        )
        self.assertEqual(request.output(), "Hi, World?")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_defaults_only_required(self, _: str, is_remote: bool):
        """Multi-param function with only required params (defaults used)."""
        if is_remote:
            deploy_applications(__file__)
        payload = {"name": "World"}
        request: Request = run_application(
            multi_params_with_defaults, payload, remote=is_remote
        )
        self.assertEqual(request.output(), "Hello, World!")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_defaults_partial(self, _: str, is_remote: bool):
        """Multi-param function with some defaults overridden."""
        if is_remote:
            deploy_applications(__file__)
        payload = {"name": "World", "greeting": "Hey"}
        request: Request = run_application(
            multi_params_with_defaults, payload, remote=is_remote
        )
        self.assertEqual(request.output(), "Hey, World!")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_pickle_with_pydantic_instances(self, _: str, is_remote: bool):
        """Pickle serializer with Pydantic model instances in payload."""
        if is_remote:
            deploy_applications(__file__)
        payload = {
            "item": Item(name="Pickled Item", price=50.0, tax=5.0),
            "user": User(username="pickler", full_name="Pete Pickler"),
            "importance": 10,
        }
        request: Request = run_application(
            multi_params_pickle, payload, remote=is_remote
        )
        output = request.output()
        self.assertEqual(output["order_summary"], "Order for Pete Pickler")
        self.assertEqual(output["item_name"], "Pickled Item")
        self.assertEqual(output["importance_level"], 10)
        self.assertEqual(output["total_price"], 55.0)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_pickle_with_dict_payload(self, _: str, is_remote: bool):
        """Pickle serializer with dict values (converted to Pydantic)."""
        if is_remote:
            deploy_applications(__file__)
        payload = {
            "item": {"name": "Dict Item", "price": 30.0, "tax": 3.0},
            "user": {"username": "dictuser", "full_name": "Dict User"},
            "importance": 3,
        }
        request: Request = run_application(
            multi_params_pickle, payload, remote=is_remote
        )
        output = request.output()
        self.assertEqual(output["order_summary"], "Order for Dict User")
        self.assertEqual(output["item_name"], "Dict Item")
        self.assertEqual(output["total_price"], 33.0)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_class_method_zero_params(self, _: str, is_remote: bool):
        """Class method with no params (excluding self)."""
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application(ZeroParamsClass().run, {}, remote=is_remote)
        self.assertEqual(request.output(), "Class value: initialized")

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_class_method_multi_params_all_provided(self, _: str, is_remote: bool):
        """Class method with multiple params all provided."""
        if is_remote:
            deploy_applications(__file__)
        payload = {
            "item": {"name": "Class Item", "price": 100.0},
            "quantity": 3,
        }
        request: Request = run_application(
            MultiParamsClass().run, payload, remote=is_remote
        )
        output = request.output()
        self.assertEqual(output["item_name"], "Class Item")
        self.assertEqual(output["quantity"], 3)
        self.assertEqual(output["total_price"], 600.0)  # 100 * 3 * 2 (multiplier)

    @parameterized.parameterized.expand([("local", False), ("remote", True)])
    def test_class_method_multi_params_with_defaults(self, _: str, is_remote: bool):
        """Class method with default param values."""
        if is_remote:
            deploy_applications(__file__)
        payload = {"item": {"name": "Default Qty", "price": 50.0}}
        request: Request = run_application(
            MultiParamsClass().run, payload, remote=is_remote
        )
        output = request.output()
        self.assertEqual(output["quantity"], 1)
        self.assertEqual(output["total_price"], 100.0)  # 50 * 1 * 2


if __name__ == "__main__":
    unittest.main()
