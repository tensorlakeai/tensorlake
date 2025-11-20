This package defined public Applications SDK interface.
No implementation details should be included here. Only the interfaces.

Sometime it's very convenient to have our internal functions/vars/classes in this
directory to avoid circular dependencies. Please add _ prefix to their names in this
case. This makes it clear that SDK users can't use them. Don't use this too much, i.e.
try to not add whole .py files prefixed with _. Ideally there would be no names starting
with _ in this directory.