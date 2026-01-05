from pydantic import BaseModel


# We're putting these models in a separate file to make sure that their classes are defined
# in python module with the same name both in test code running in __main__ module and in
# tensorlake functions code running in module with .py test file name.
#
# This is required when i.e. pickling a model instance in test code __main__ module and
# unpickling it in tensorlake function code module with .py test file name.
class FileModel(BaseModel):
    path: str
    size: int
    is_read_only: bool


class DirModel(BaseModel):
    path: str
    files: list[FileModel]
