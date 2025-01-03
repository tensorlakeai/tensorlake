# Reference test graph for developing tensorlake CLI.
# This is what we expect the user experience is going to look like and what will guide the development of the tensorlake CLI.

# Running "tensorlake prepare test_graph.py" will build the images and make sure everything is ready.
# Running "tensrolake deploy test_graph.py" will check everything is ready and deploy the graph to tensorlake.

from pydantic import BaseModel
from indexify import indexify_function, Graph, Image # This should be tensorlake
from typing import List

mapper_image = Image().name("generator").run("pip install httpx")
process_image = Image().name("process").run("pip install numpy")
reducer_image = Image().name("adder").run("pip install httpx")

class Total(BaseModel):
    val: int = 0

@indexify_function(image=mapper_image) # Are these going to be tensorlake_function? 
def map_function(a: int) -> List[int]:
    return [i for i in range(a)]

@indexify_function()
def process_function(x: int) -> int:
    return x ** 2

@indexify_function(accumulate=Total, image=reducer_image)
def reduce_function(total: Total, new: int) -> Total:
    total.val += new
    return total

g = Graph(name="sequence_summer", start_node=map_function, description="Simple Sequence Summer")
g.add_edge(map_function, process_function)
g.add_edge(process_function, reduce_function)

if __name__ == "__main__":
    g.local().invoke(a=10) # Runs the graph locally, block.

    # Do we need to login at some point or just rely on the env var?
    # tensorlake.login()

    # tensorlake.ping() # Throw an exception if we can't communicate with the platform

    g.deploy() # This will deploy the graph to tensorlake, includes generating any images. 
               # Should be a no-op if there aren't any changes.
               # What is the behavior if we try and invoke the graph before deploying it?
    
    inv = g.remote().invoke(a=10) # This will invoke the graph remotely, block.
                                  # If the graph fails because of user error we should raise an exception here?
     
    inv.raise_for_status() # This will raise an exception if the invocation failed?

    inv = g.remote().invoke(a=10, block_until_done=False) # This will invoke the graph remotely, non-blocking.
    
    # If in non-blocking mode the user needs to poll inv.status() to check if the invocation is done.
    while inv.status() != "done": # The value of "status" will be an enum
        print("Waiting for invocation to complete.")
    
    print(inv.result()) # This will print the result of the invocation.
    print(inv.logs()) # This will print the logs of the invocation.


