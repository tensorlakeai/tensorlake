import unittest

import parameterized
from pydantic import BaseModel

from tensorlake.applications import (
    Request,
    RequestContext,
    RequestError,
    api,
    call_api,
    function,
)
from tensorlake.applications.remote.deploy import deploy


@api()
@function()
def test_request_context_state_set_get_simple_value_api(value: int) -> str:
    ctx: RequestContext = RequestContext.get()
    ctx.state.set("key1", value)
    return test_request_context_state_set_get_simple_value_internal()


@function()
def test_request_context_state_set_get_simple_value_internal() -> int:
    ctx: RequestContext = RequestContext.get()
    return ctx.state.get("key1")


@api()
@function()
def test_request_context_state_get_default_value_api(default: str) -> str:
    ctx: RequestContext = RequestContext.get()
    return ctx.state.get("non_existing_key", default)


@api()
@function()
def test_request_context_state_get_without_default_value_returns_none_api(
    _: str,
) -> None:
    ctx: RequestContext = RequestContext.get()
    return ctx.state.get("non_existing_key")


# Serializing user-defined class instances should work because we're using Pydantic serializer for request state.
# This gives the most flexible UX to users.
class UserClass:
    def __init__(self, times: int):
        self._times: int = times
        self._data: bytes = b"data" * times


@api()
@function()
def test_request_context_state_set_get_user_class_instance_api(times: int) -> str:
    ctx: RequestContext = RequestContext.get()
    user_instance: UserClass = UserClass(times)
    ctx.state.set("user_class", user_instance)
    return test_request_context_state_set_get_user_class_instance_internal(times)


@function()
def test_request_context_state_set_get_user_class_instance_internal(times: int) -> str:
    ctx: RequestContext = RequestContext.get()
    user_instance: UserClass = ctx.state.get("user_class")
    if not isinstance(user_instance, UserClass):
        raise RequestError("user_instance is not of type UserClass")

    if user_instance._times != times:
        raise RequestError("user_instance._times is not equal to times")

    if user_instance._data != b"data" * times:
        raise RequestError("user_instance._data is corrupted")

    return "success"


class UserModel(BaseModel):
    id: int
    name: str


@api()
@function()
def test_request_context_state_set_get_pydantic_model_api(model_name: str) -> str:
    user_model: UserModel = UserModel(id=1, name=model_name)
    ctx: RequestContext = RequestContext.get()
    ctx.state.set("user_model", user_model)
    return test_request_context_state_set_get_pydantic_model_internal(model_name)


@function()
def test_request_context_state_set_get_pydantic_model_internal(model_name: str) -> str:
    ctx: RequestContext = RequestContext.get()
    user_model: UserModel = ctx.state.get("user_model")
    if not isinstance(user_model, UserModel):
        raise RequestError("user_model is not of type UserModel")

    if user_model.id != 1:
        raise RequestError("user_model.id is not equal to 1")

    if user_model.name != model_name:
        raise RequestError("user_model.name is not equal to model_name")

    return "success"


class TestRequestContext(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_context_state_set_get_simple_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            test_request_context_state_set_get_simple_value_api, 11, remote=is_remote
        )

        output: int = request.output()
        self.assertEqual(output, 11)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_context_state_set_get_user_class_instance(
        self, _: str, is_remote: bool
    ):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            test_request_context_state_set_get_user_class_instance_api,
            11,
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_context_state_set_get_pydantic_model(
        self, _: str, is_remote: bool
    ):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            test_request_context_state_set_get_pydantic_model_api,
            "test_model_name",
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "success")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_context_state_get_default_value(self, _: str, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            test_request_context_state_get_default_value_api,
            "default_value",
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "default_value")

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_request_context_state_get_without_default_value_returns_none(
        self, _: str, is_remote: bool
    ):
        if is_remote:
            deploy(__file__)

        request: Request = call_api(
            test_request_context_state_get_without_default_value_returns_none_api,
            None,
            remote=is_remote,
        )

        output: None | str = request.output()
        self.assertIsNone(output)


if __name__ == "__main__":
    unittest.main()
