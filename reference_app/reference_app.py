# Reference test application for developing tensorlake CLI.
# This is what we expect the user experience is going to look like and what will guide the development of the tensorlake CLI.

# Running "tensorlake prepare reference_app/reference_app.py" will build the images and make sure everything is ready.
# Running "tensorlake deploy reference_app/reference_app.py" will check everything is ready and deploy the application to tensorlake.
from pydantic import BaseModel

from tensorlake.applications import (
    Image,
    Request,
    application,
    function,
)
from tensorlake.applications import map as tl_map
from tensorlake.applications import reduce as tl_reduce
from tensorlake.applications import (
    run_remote_application,
)

mapper_image = Image(name="generator").run("pip install httpx")
process_image = Image(name="process").run("pip install numpy")
reducer_image = Image(name="adder").run("pip install httpx")


class Total(BaseModel):
    val: int = 0


@application()
@function(image=mapper_image, description="Sums the squares of a sequence of numbers")
def sequence_summer(a: int) -> Total:
    return tl_reduce(reducer, tl_map(processor, range(a)))


@function(image=process_image)
def processor(x: int) -> Total:
    return Total(val=x**2)


@function(image=reducer_image)
def reducer(total: Total, new: Total) -> Total:
    total.val += new.val
    return total


if __name__ == "__main__":
    request: Request = run_remote_application(sequence_summer, 10)
    assert request.output().val == sum(x**2 for x in range(10))
    print("Request completed successfully with output:", request.output())
