# @function()
# @application()
# def test_set_arg_and_return_value_api(arg: set[str]) -> set[str]:
#     if not isinstance(arg, set):
#         raise RequestError(f"Set type mismatch: {type(arg)}")
#     if arg != {"apple", "banana", "cherry"}:
#         raise RequestError(f"Set content mismatch: {arg}")
#     return arg

#     @parameterized.parameterized.expand([("remote", True), ("local", False)])
#     def test_set_arg_and_return_value(self, _: str, is_remote: bool):
#         if is_remote:
#             deploy_applications(__file__)

#         request: Request = run_application(
#             test_set_arg_and_return_value_api,
#             is_remote,
#             {"apple", "banana", "cherry"},
#         )
#         output_set: set[str] = request.output()
#         self.assertIsInstance(output_set, set)
#         self.assertEqual(output_set, {"apple", "banana", "cherry"})
