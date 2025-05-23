from typing import List

# Import from local package using absolute import path, should work because the code dir zip is added to PYTHONPATH
# and because the local package has __init__.py file.
from hello_world.hello_world import hello_world


def repeat_hello_world(times: int) -> List[str]:
    return [hello_world()] * times
