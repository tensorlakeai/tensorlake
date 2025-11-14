from tensorlake.applications import (
    application,
    cls,
    function,
)


@cls()
class Class1:
    @application()
    @function()
    def method(self, _: str) -> str:
        return "class_1.py.Class1.method"
