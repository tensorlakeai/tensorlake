import inspect
import os
from typing import Any, Callable

import pydantic

from ..function.introspect import (
    ClassDetails,
    FunctionDetails,
    get_class_details,
    get_function_details,
    is_module_level_function_or_class,
)
from ..function.type_hints import (
    function_signature,
    is_awaitable_type_hint,
    is_file_type_hint,
)
from ..function.user_data_serializer import function_input_serializer
from ..interface import Awaitable, File, Function, InternalError, SerializationError
from ..interface.decorators import (
    _ApplicationDecorator,
    _class_name,
    _ClassDecorator,
    _Decorator,
    _FunctionDecorator,
)
from ..interface.function import _is_application_function
from ..registry import (
    get_classes,
    get_classes_with_duplicates,
    get_decorators,
    get_functions,
    get_functions_with_duplicates,
    has_class,
)
from ..user_data_serializer import (
    UserDataSerializer,
    create_type_adapter,
    generate_json_schema,
)
from .message import ValidationMessage, ValidationMessageSeverity


def validate_loaded_applications() -> list[ValidationMessage]:
    """Validates applications loaded into current Python process.

    All validation messages are returned in the list.
    The validation is performed on static aspects of the loaded applications.
    It's not possible to validate runtime aspects like if a function returns
    a valid object.
    """
    messages: list[ValidationMessage] = []
    messages.extend(_validate_decorator_calls())
    messages.extend(_validate_function_duplicates())
    messages.extend(_validate_class_duplicates())
    messages.extend(_validate_classes())
    messages.extend(_validate_applications())
    messages.extend(_validate_functions())
    return messages


def _details_for_decorator_chain(
    object: (
        _FunctionDecorator
        | _ApplicationDecorator
        | _ClassDecorator
        | Function
        | Callable
        | Any
    ),
) -> FunctionDetails | ClassDetails:
    """Returns details for an object that can be used in a decorator chain.

    Works correctly when the decorator wasn't called by user with ().
    """
    if isinstance(object, _FunctionDecorator):
        if object.is_called:
            return _details_for_decorator_chain(object.function)
        else:
            # If the decorator wasn't called by user with () then it's first constructor argument
            # is the decorated function itself.
            return _details_for_decorator_chain(object._description)
    elif isinstance(object, _ApplicationDecorator):
        if object.is_called:
            return _details_for_decorator_chain(object.function)
        else:
            # If the decorator wasn't called by user with () then it's first constructor argument
            # is the decorated function itself.
            return _details_for_decorator_chain(object._tags)
    elif isinstance(object, _ClassDecorator):
        if object.is_called:
            return _details_for_decorator_chain(object.original_class)
        else:
            # If the decorator wasn't called by user with () then it's first constructor argument
            # is the decorated class itself.
            return _details_for_decorator_chain(object._init_timeout)
    elif isinstance(object, Function):
        return get_function_details(object._original_function)
    elif inspect.isclass(object):
        return get_class_details(object)
    else:
        # Assuming regular function aka Callable.
        return get_function_details(object)


def _validate_decorator_calls() -> list[ValidationMessage]:
    """Validates that every decorator was called by user using () applied to it."""
    messages: list[ValidationMessage] = []

    for decorator in get_decorators():
        decorator: _Decorator
        if decorator.is_called:
            continue

        if isinstance(decorator, _FunctionDecorator):
            fn_details: FunctionDetails = _details_for_decorator_chain(decorator)
            messages.append(
                ValidationMessage(
                    message="@function decorator is missing its parenthesis. Please replace it with @function().",
                    severity=ValidationMessageSeverity.ERROR,
                    details=fn_details,
                )
            )
        elif isinstance(decorator, _ApplicationDecorator):
            fn_details: FunctionDetails = _details_for_decorator_chain(decorator)
            messages.append(
                ValidationMessage(
                    message="@application decorator is missing its parenthesis. Please replace it with @application().",
                    severity=ValidationMessageSeverity.ERROR,
                    details=fn_details,
                )
            )
        elif isinstance(decorator, _ClassDecorator):
            cls_details: ClassDetails = _details_for_decorator_chain(decorator)
            messages.append(
                ValidationMessage(
                    message="@cls decorator is missing its parenthesis. Please replace it with @cls().",
                    severity=ValidationMessageSeverity.ERROR,
                    details=cls_details,
                )
            )
        else:
            raise InternalError(
                f"Unknown decorator type: {type(decorator)} for decorator: {decorator}"
            )

    return messages


def _validate_function_duplicates() -> list[ValidationMessage]:
    """Validates that all duplicated function definitions are exactly for the same functions.

    Returns a list of validation messages.
    """
    messages: list[ValidationMessage] = []

    for fn_name, fn_list in get_functions_with_duplicates().items():
        fn_src_abs_paths: set[str] = set()
        for fn in fn_list:
            fn: Function
            # Use fn._original_function in all places because it's always set in Function.
            fn_details: FunctionDetails = get_function_details(fn._original_function)
            if fn_details.class_name is not None:
                # Will be handled by class level deduplication.
                continue

            fn_src_path: str = fn_details.source_file_path
            try:
                fn_src_path = os.path.abspath(fn_src_path)
            except Exception:
                pass  # Use non-absolute path if conversion fails for any reason.
            fn_src_abs_paths.add(fn_src_path)

        # Allow re-registering the same function from the same file.
        # This is needed because pickle.loads imports __main__ module
        # second time but with its real name (i.e. real_name.py) when unpickling
        # classes stored in function call and application entrypoint metadata.
        # So two modules exist for real_name.py in sys.modules:
        # * __main__
        # * real_name
        # Another legitimate use case if when user redefines the function
        # in the same file. This is a valid Python code.
        if len(fn_src_abs_paths) < 2:
            continue  # All function definitions are from the same file or class methods.

        messages.append(
            ValidationMessage(
                message=f"Function '{fn_name}' is defined in files: {', '.join(fn_src_abs_paths)}. "
                "Functions with the same names can't be defined in different files. Please rename the functions.",
                severity=ValidationMessageSeverity.ERROR,
                details=fn_details,
            )
        )

    return messages


def _validate_class_duplicates() -> list[ValidationMessage]:
    """Validates that all duplicated class definitions are exactly for the same classes.

    Returns a list of validation messages.
    """
    messages: list[ValidationMessage] = []

    for _, cls_list in get_classes_with_duplicates().items():
        # At list one item in cls_list.
        cls_src_abs_paths: set[str] = set()
        for cls in cls_list:
            cls_details: ClassDetails = get_class_details(cls)
            cls_src_path: str = cls_details.source_file_path
            try:
                cls_src_path = os.path.abspath(cls_src_path)
            except Exception:
                pass  # Use non-absolute path if conversion fails for any reason.
            cls_src_abs_paths.add(cls_src_path)

        # Allow re-registering the same class from the same file.
        # This is needed because pickle.loads imports __main__ module
        # second time but with its real name (i.e. real_name.py) when unpickling
        # classes stored in function call and application entrypoint metadata.
        # So two modules exist for real_name.py in sys.modules:
        # * __main__
        # * real_name
        # Another legitimate use case if when user redefines the class
        # in the same file. This is a valid Python code.

        if len(cls_src_abs_paths) == 1:
            continue  # All class definitions are from the same file.

        messages.append(
            ValidationMessage(
                message=f"Class '{cls_details.class_name}' is defined in files: {', '.join(cls_src_abs_paths)}. "
                "Classes with the same names can't be defined in different files. Please rename the classes.",
                severity=ValidationMessageSeverity.ERROR,
                details=cls_details,
            )
        )

    return messages


def _validate_classes() -> list[ValidationMessage]:
    """Validates all loaded classes.

    Returns a list of validation messages.
    """
    messages: list[ValidationMessage] = []
    for cls in get_classes():
        messages.extend(_validate_class(cls))

    return messages


def _validate_class(cls: Any) -> list[ValidationMessage]:
    """Validates a single class."""
    messages: list[ValidationMessage] = []

    if not inspect.isclass(cls):
        # Python allows decorating classes, functions and methods only.
        # cls could also be a Function object or not called Decorator.
        function_details: FunctionDetails = _details_for_decorator_chain(cls)
        messages.append(
            ValidationMessage(
                message="@cls() is applied to function. Please use @cls() only on classes.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )
        # Return immediately because rest of validations don't make sense.
        return messages

    cls_details: ClassDetails = get_class_details(cls)
    init_signature: inspect.Signature = inspect.signature(
        cls.__tensorlake_original_init__
    )
    init_parameters: list[inspect.Parameter] = list(init_signature.parameters.values())

    if len(init_parameters) == 0:
        messages.append(
            ValidationMessage(
                message=f"'{_class_name(cls)}.__init__' is missing 'self' parameter. Please add 'self' parameter.",
                severity=ValidationMessageSeverity.ERROR,
                details=cls_details,
            )
        )
    else:
        if init_parameters[0].name != "self":
            messages.append(
                ValidationMessage(
                    message=f"'{_class_name(cls)}.__init__' should have its first parameter named 'self'. Please rename the first parameter to 'self'.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=cls_details,
                )
            )

        if len(init_parameters) > 1:
            # When no __init__ method is defined by user code, it's actual signature is __init__(self, /, *args, **kwargs). We allow that.
            if not (
                len(init_parameters) == 3
                and init_parameters[1].kind == inspect.Parameter.VAR_POSITIONAL
                and init_parameters[2].kind == inspect.Parameter.VAR_KEYWORD
            ):
                messages.append(
                    ValidationMessage(
                        message=f"'{_class_name(cls)}.__init__' can only have a single 'self' parameter. Please remove the rest of the parameters.",
                        severity=ValidationMessageSeverity.ERROR,
                        details=cls_details,
                    )
                )

    if not is_module_level_function_or_class(cls):
        messages.append(
            ValidationMessage(
                message="Only module level classes are supported. Please move the class to module level by i.e. "
                "moving it outside of the function where it is defined.",
                severity=ValidationMessageSeverity.ERROR,
                details=cls_details,
            )
        )

    return messages


def _validate_applications() -> list[ValidationMessage]:
    """Validates global application settings.

    Returns a list of validation messages.
    """
    messages: list[ValidationMessage] = []
    applications: list[Function] = []

    has_application_decorator: bool = any(
        isinstance(d, _ApplicationDecorator) for d in get_decorators()
    )
    for function in get_functions():
        if _is_application_function(function):
            applications.append(function)

    if not has_application_decorator and len(applications) == 0:
        messages.append(
            ValidationMessage(
                message="No application function is defined. Please add at least one application function by adding @application() decorator to it.",
                severity=ValidationMessageSeverity.ERROR,
                details=None,
            )
        )

    return messages


def _validate_functions() -> list[ValidationMessage]:
    """Validates all loaded functions.

    Returns a list of validation messages.
    """
    messages: list[ValidationMessage] = []
    for fn in get_functions():
        messages.extend(_validate_function(fn))
    return messages


def _validate_function(function: Function) -> list[ValidationMessage]:
    """Validates a single function."""
    messages: list[ValidationMessage] = []
    # Use function._original_function in all places because it's always set in Function.
    function_details: FunctionDetails = get_function_details(
        function._original_function
    )
    messages.extend(_validate_regular_function(function, function_details))
    if _is_application_function(function):
        messages.extend(_validate_application_function(function, function_details))
    if function_details.class_name is not None:
        messages.extend(_validate_method_function(function, function_details))

    return messages


def _validate_regular_function(
    function: Function, function_details: FunctionDetails
) -> list[ValidationMessage]:
    """Validates aspects of a regular function."""
    messages: list[ValidationMessage] = []

    if inspect.isclass(function._original_function):
        # Python allows decorating classes, functions and methods only.
        class_details: ClassDetails = get_class_details(function._original_function)
        messages.append(
            ValidationMessage(
                message="@function() decorator is applied to class. Please use @function() only on functions and class methods.",
                severity=ValidationMessageSeverity.ERROR,
                details=class_details,
            )
        )
        # Return immediately because rest of validations don't make sense.
        return messages

    if function_details.class_name is None and not is_module_level_function_or_class(
        function._original_function
    ):
        messages.append(
            ValidationMessage(
                message="Only module level functions are supported. Please move the function to module level by i.e. "
                "moving it outside of the function where it is defined.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )

    return messages


def _validate_application_function(
    function: Function, function_details: FunctionDetails
) -> list[ValidationMessage]:
    """Validates application aspects of an application function."""
    messages: list[ValidationMessage] = []

    function_decorator: _FunctionDecorator | None = None
    for decorator in get_decorators():
        if isinstance(decorator, _FunctionDecorator):
            decorator_function_details: FunctionDetails = _details_for_decorator_chain(
                decorator
            )
            if decorator_function_details == function_details:
                function_decorator = decorator
                break

    if function_decorator is None:
        messages.append(
            ValidationMessage(
                message="Application function is missing @function() decorator. "
                "Please add it. An application function needs both @application() and @function() decorators.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )

    try:
        signature: inspect.Signature = function_signature(function)
    except Exception as e:
        messages.append(
            ValidationMessage(
                message=f"Failed to get signature of application function: {e}. "
                "Please make sure that the application function is defined correctly.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )
        # Return immediately because rest of validations don't make sense.
        return messages

    first_arg_index: int = 0 if function_details.class_name is None else 1
    # signature.parameters is an ordered mapping in parameters definition order.
    parameters_in_definition_order: list[inspect.Parameter] = list(
        signature.parameters.values()
    )[first_arg_index:]
    for parameter in parameters_in_definition_order:
        parameter: inspect.Parameter

        # This is required to not deal with complexities of POSITIONAL_ONLY and KEYWORD_ONLY parameters
        # in application function calling conventions.
        if parameter.kind != inspect.Parameter.POSITIONAL_OR_KEYWORD:
            messages.append(
                ValidationMessage(
                    message=f"Application function parameter '{parameter.name}' has unsupported parameter kind '{parameter.kind.description}'. "
                    "Please change the parameter to be positional or keyword (the default kind for Python parameters).",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )

        if parameter.annotation is inspect.Parameter.empty:
            messages.append(
                ValidationMessage(
                    message=f"Application function parameter '{parameter.name}' requires a type hint. Please add a type hint to the parameter.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )
        else:
            if is_file_type_hint(parameter.annotation):
                continue  # Not serialized using Pydantic

            try:
                type_adapter: pydantic.TypeAdapter = create_type_adapter(
                    parameter.annotation
                )
            except Exception as e:
                if str(File) in str(e):
                    messages.append(
                        ValidationMessage(
                            message=f"Application function parameter '{parameter.name}' uses a File object in a complex type hint '{parameter.annotation}'. "
                            f"Only simple type hints like '{parameter.name}: File' are supported for File objects in application function parameters. "
                            f"A File object cannot be embedded inside other type hints like '{parameter.name}: list[File]', '{parameter.name}: dict[str, File]', etc or be a part of union "
                            f"type hint like '{parameter.name}: File | None'.",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )
                else:
                    messages.append(
                        ValidationMessage(
                            message=f"Pydantic failed to generate CoreSchema for Application function parameter '{parameter.name}' "
                            f"with type hint '{parameter.annotation}'. Please follow Pydantic documentation and the following Pydantic "
                            f"error message to make the type hint compatible.\nPydantic error message:\n{e}\n",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )
            else:
                try:
                    generate_json_schema(type_adapter)
                except Exception as e:
                    messages.append(
                        ValidationMessage(
                            message=f"Pydantic failed to generate JSON schema for Application function parameter '{parameter.name}' "
                            f"with type hint '{parameter.annotation}'. Please follow Pydantic documentation and the following Pydantic "
                            f"error message to make the type hint serializable to JSON format.\nPydantic error message:\n{e}\n",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )

                if parameter.default is not inspect.Parameter.empty:
                    default_arg_value_serializer: UserDataSerializer = (
                        function_input_serializer(function, app_call=True)
                    )
                    try:
                        default_arg_value_serializer.serialize(
                            parameter.default, parameter.annotation
                        )
                    except SerializationError as e:
                        messages.append(
                            ValidationMessage(
                                message=f"Application function parameter '{parameter.name}' has a default value that can't be serialized with "
                                f"the type hint of the parameter {parameter.annotation}. "
                                f"Please follow Pydantic documentation and the following SerializationError to make the default value serializable "
                                f"to JSON format:\n{e}\n",
                                severity=ValidationMessageSeverity.ERROR,
                                details=function_details,
                            )
                        )

    if signature.return_annotation is inspect.Signature.empty:
        messages.append(
            ValidationMessage(
                message="Application function requires a return type hint. Please add a return type hint to the function.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )
    else:
        if is_file_type_hint(signature.return_annotation):
            pass  # Not serialized using Pydantic
        elif is_awaitable_type_hint(signature.return_annotation):
            messages.append(
                ValidationMessage(
                    message=f"Application function return type hint is an Awaitable. "
                    "Instead of using Awaitable as a return type hint, please use type hint of the value returned by the Awaitable. "
                    "For example, if the Awaitable (once resolved) returns str, then please use 'str' in the return type hint of "
                    "the application function instead of the Awaitable.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )
        else:
            try:
                type_adapter: pydantic.TypeAdapter = create_type_adapter(
                    signature.return_annotation
                )
            except Exception as e:
                e_str: str = str(e)
                if str(File) in e_str:
                    messages.append(
                        ValidationMessage(
                            message=f"Application function return type hint '{signature.return_annotation}' uses a File object in a complex type hint. "
                            "Only simple type hints like 'foo: File' are supported for File objects in application function return types. "
                            "A File object cannot be embedded inside other type hints like 'foo: list[File]', 'foo: dict[str, File]', etc or be a part of union "
                            "type hint like 'foo: File | None'.",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )
                elif str(Awaitable) in e_str:
                    messages.append(
                        ValidationMessage(
                            message=f"Application function return type hint '{signature.return_annotation}' uses an Awaitable object. "
                            "Instead of using Awaitable in the return type hint, please use type hint of the value returned by the Awaitable. "
                            "For example, if the Awaitable (once resolved) returns str, then please use 'str' in the return type hint of "
                            "the application function instead of the Awaitable.",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )
                else:
                    messages.append(
                        ValidationMessage(
                            message=f"Pydantic failed to generate CoreSchema for Application function return type hint '{signature.return_annotation}'. "
                            "Please follow Pydantic documentation and the following Pydantic error message to make the type hint compatible."
                            f"\nPydantic error message:\n{e_str}\n",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )
            else:
                try:
                    generate_json_schema(type_adapter)
                except Exception as e:
                    messages.append(
                        ValidationMessage(
                            message=f"Pydantic failed to generate JSON schema for Application function return type hint '{signature.return_annotation}'. "
                            "Please follow Pydantic documentation and the following Pydantic error message to make the type hint serializable to JSON format."
                            f"\nPydantic error message:\n{e}\n",
                            severity=ValidationMessageSeverity.ERROR,
                            details=function_details,
                        )
                    )

    return messages


def _validate_method_function(
    function: Function, function_details: FunctionDetails
) -> list[ValidationMessage]:
    """Validates aspects of a class method function."""
    messages: list[ValidationMessage] = []

    if (
        "<locals>" not in function_details.class_name
        and len(function_details.class_name.split(".")) > 1
    ):
        # Nested class.
        messages.append(
            ValidationMessage(
                message="Function is defined inside a nested class. Nested classes are not supported. Please move the function to a top level class.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )
        # Return immediately because rest of validations don't make sense due to @cls() decorators mismatch.
        return messages

    has_cls_decorator: bool = has_class(function_details.class_name)
    for decorator in get_decorators():
        if isinstance(decorator, _ClassDecorator):
            cls_details: ClassDetails = _details_for_decorator_chain(decorator)
            if cls_details.class_name == function_details.class_name:
                has_cls_decorator = True
                break

    if not has_cls_decorator:
        messages.append(
            ValidationMessage(
                message=f"Please add @cls() decorator to class '{function_details.class_name}' where the function is defined.",
                severity=ValidationMessageSeverity.ERROR,
                details=function_details,
            )
        )

    # Application method signature is verified by application function validation.
    if not _is_application_function(function):
        signature: inspect.Signature = inspect.signature(function._original_function)
        parameters: list[inspect.Parameter] = list(signature.parameters.values())
        if len(parameters) < 1 or parameters[0].name != "self":
            messages.append(
                ValidationMessage(
                    message="Function is a class method so it needs a 'self' parameter. Please add 'self' as the first parameter of the function.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )

    return messages
