# @function()
# @application()
# def test_tuple_arg_and_return_value_api(
#     arg: tuple[str, str, str, str],
# ) -> tuple[str, str, str, str]:
#     if not isinstance(arg, tuple):
#         raise RequestError(f"Tuple type mismatch: {type(arg)}")
#     if arg != ("apple", "banana", "cherry", "cherry"):
#         raise RequestError(f"Tuple content mismatch: {arg}")
#     return arg

#     @parameterized.parameterized.expand([("remote", True), ("local", False)])
#     def test_tuple_arg_and_return_value(self, _: str, is_remote: bool):
#         if is_remote:
#             deploy_applications(__file__)

#         request: Request = run_application(
#             test_tuple_arg_and_return_value_api,
#             is_remote,
#             ("apple", "banana", "cherry", "cherry"),
#         )
#         output_tuple: tuple[str, str, str, str] = request.output()
#         self.assertIsInstance(output_tuple, tuple)
#         self.assertEqual(output_tuple, ("apple", "banana", "cherry", "cherry"))
