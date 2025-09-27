import unittest

import parameterized
from pydantic import BaseModel

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api()
@tensorlake.function()
def test_request_context_state_set_get_simple_value_api(
    ctx: tensorlake.RequestContext, value: int
) -> str:
    ctx.state.set("key1", value)
    return test_request_context_state_set_get_simple_value_internal(ctx)


@tensorlake.function()
def test_request_context_state_set_get_simple_value_internal(
    ctx: tensorlake.RequestContext,
) -> int:
    return ctx.state.get("key1")


@tensorlake.api()
@tensorlake.function()
def test_request_context_state_get_default_value_api(
    ctx: tensorlake.RequestContext, default: str
) -> str:
    return ctx.state.get("non_existing_key", default)


@tensorlake.api()
@tensorlake.function()
def test_request_context_state_get_without_default_value_returns_none_api(
    ctx: tensorlake.RequestContext, _: str
) -> None:
    return ctx.state.get("non_existing_key")


# Serializing user-defined class instances should work because we're using Pydantic serializer for request state.
# This gives the most flexible UX to users.
class UserClass:
    def __init__(self, times: int):
        self._times: int = times
        self._data: bytes = b"data" * times


@tensorlake.api()
@tensorlake.function()
def test_request_context_state_set_get_user_class_instance_api(
    ctx: tensorlake.RequestContext, times: int
) -> str:
    user_instance: UserClass = UserClass(times)
    ctx.state.set("user_class", user_instance)
    return test_request_context_state_set_get_user_class_instance_internal(ctx, times)


@tensorlake.function()
def test_request_context_state_set_get_user_class_instance_internal(
    ctx: tensorlake.RequestContext, times: int
) -> str:
    user_instance: UserClass = ctx.state.get("user_class")
    if not isinstance(user_instance, UserClass):
        raise tensorlake.RequestError("user_instance is not of type UserClass")

    if user_instance._times != times:
        raise tensorlake.RequestError("user_instance._times is not equal to times")

    if user_instance._data != b"data" * times:
        raise tensorlake.RequestError("user_instance._data is corrupted")

    return "success"


class UserModel(BaseModel):
    id: int
    name: str


@tensorlake.api()
@tensorlake.function()
def test_request_context_state_set_get_pydantic_model_api(
    ctx: tensorlake.RequestContext, model_name: str
) -> str:
    user_model: UserModel = UserModel(id=1, name=model_name)
    ctx.state.set("user_model", user_model)
    return test_request_context_state_set_get_pydantic_model_internal(ctx, model_name)


@tensorlake.function()
def test_request_context_state_set_get_pydantic_model_internal(
    ctx: tensorlake.RequestContext, model_name: str
) -> str:
    user_model: UserModel = ctx.state.get("user_model")
    if not isinstance(user_model, UserModel):
        raise tensorlake.RequestError("user_model is not of type UserModel")

    if user_model.id != 1:
        raise tensorlake.RequestError("user_model.id is not equal to 1")

    if user_model.name != model_name:
        raise tensorlake.RequestError("user_model.name is not equal to model_name")

    return "success"


class TestRequestContext(unittest.TestCase):
    @parameterized.parameterized.expand([(False), (True)])
    def test_request_context_state_set_get_simple_value(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_request_context_state_set_get_simple_value_api, 11, remote=is_remote
        )

        output: int = request.output()
        self.assertEqual(output, 11)

    @parameterized.parameterized.expand([(False), (True)])
    def test_request_context_state_set_get_user_class_instance(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_request_context_state_set_get_user_class_instance_api,
            11,
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "success")

    @parameterized.parameterized.expand([(False), (True)])
    def test_request_context_state_set_get_pydantic_model(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_request_context_state_set_get_pydantic_model_api,
            "test_model_name",
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "success")

    @parameterized.parameterized.expand([(False), (True)])
    def test_request_context_state_get_default_value(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_request_context_state_get_default_value_api,
            "default_value",
            remote=is_remote,
        )

        output: str = request.output()
        self.assertEqual(output, "default_value")

    @parameterized.parameterized.expand([(False), (True)])
    def test_request_context_state_get_without_default_value_returns_none(
        self, is_remote: bool
    ):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            test_request_context_state_get_without_default_value_returns_none_api,
            None,
            remote=is_remote,
        )

        output: None | str = request.output()
        self.assertIsNone(output)


if __name__ == "__main__":
    unittest.main()
