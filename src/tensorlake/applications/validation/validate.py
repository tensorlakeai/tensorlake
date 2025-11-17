import inspect
import os
from typing import Any, Callable

from ..function.introspect import (
    ClassDetails,
    FunctionDetails,
    get_class_details,
    get_function_details,
    is_application_function,
    is_module_level_function_or_class,
)
from ..function.type_hints import (
    function_arg_type_hint,
    function_return_type_hint,
    function_signature,
)
from ..interface import Function
from ..interface.decorators import (
    _ApplicationDecorator,
    _class_name,
    _ClassDecorator,
    _Decorator,
    _FunctionDecorator,
)
from ..registry import (
    get_classes,
    get_classes_with_duplicates,
    get_decorators,
    get_functions,
    get_functions_with_duplicates,
    has_class,
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
            raise ValueError(
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

    has_application: bool = any(
        isinstance(d, _ApplicationDecorator) for d in get_decorators()
    )
    for function in get_functions():
        function: Function
        if is_application_function(function):
            has_application = True
            break

    if not has_application:
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
    if is_application_function(function):
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
    """Validates a application aspects of an application function."""
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

    signature: inspect.Signature = function_signature(function)
    signature_is_valid: bool = True
    if function_details.class_name is None:
        if len(signature.parameters) != 1:
            signature_is_valid = False
            messages.append(
                ValidationMessage(
                    message="Application function needs to have exactly one parameter (aka request input). "
                    "Please change the function parameters. Non-application functions don't have this limitation.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )
    else:
        if len(signature.parameters) != 2:
            signature_is_valid = False
            messages.append(
                ValidationMessage(
                    message="Application function needs to have exactly two parameters (self and request input). "
                    "Please change the function parameters. Non-application functions don't have this limitation.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )

    if signature_is_valid:
        # Warning: if you want to delete this or reduce severity then add a test that verifies that things work without type hints.
        request_input_type_hints: list[Any] = function_arg_type_hint(function, -1)
        if len(request_input_type_hints) == 0:
            messages.append(
                ValidationMessage(
                    message="Application function parameter requires a type hint. Please add a type hint to the parameter.",
                    severity=ValidationMessageSeverity.ERROR,
                    details=function_details,
                )
            )

    # Warning: if you want to delete this or reduce severity then add a test that verifies that things work without type hints.
    return_type_hints: list[Any] = function_return_type_hint(function)
    if len(return_type_hints) == 0:
        messages.append(
            ValidationMessage(
                message="Application function requires a return type hint. Please add a return type hint to the function.",
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
    if not is_application_function(function):
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
