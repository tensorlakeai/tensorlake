# Reference test graph for developing indexify CLI.
# This is what we expect the user experience is going to look like and what will guide the development of the indexify CLI.

# Running "indexify prepare reference_graph.py" will build the images and make sure everything is ready.
# Running "tensrolake deploy reference_graph.py" will check everything is ready and deploy the graph to indexify.

from typing import List

from pydantic import BaseModel

from tensorlake import Graph, Image, tensorlake_function

# curl -H 'Authorization: Bearer tl_apiKey_7PmWt8MDzJmDjM7tht6zh_9BYiitvUcvjuP3QUq5poL_lAV5Owfp' https://api.tensorlake.ai/namespaces/project_9fHtqMqGw8FNHWCk9n6qf/compute_graphs | jq
# curl  http://localhost:8900/namespaces/project_9fHtqMqGw8FNHWCk9n6qf/compute_graphs | jq
mapper_image = Image().name("generator").run("pip install httpx numpy")
process_image = Image().name("process").run("pip install numpy")
reducer_image = (
    Image().name("adder").run("pip install httpx numpy")
)  # .run("pip install numpy requests notAPackage")


class Total(BaseModel):
    val: int = 0


@tensorlake_function(image=mapper_image)  # Are these going to be indexify_function?
def map_function(a: int) -> List[int]:
    return [i for i in range(a)]


@tensorlake_function(image=process_image)
def process_function(x: int) -> int:
    return x**2


@tensorlake_function(accumulate=Total, image=reducer_image)
def reduce_function(total: Total, new: int) -> Total:
    total.val += new
    return total


summer = Graph(
    name="sequence_summer",
    start_node=map_function,
    description="Simple Sequence Summer",
)
summer.add_edge(map_function, process_function)
summer.add_edge(process_function, reduce_function)

# if __name__ == "__main__":
#     # For serverless
#     inv = summer.queue(a=10) # Runs the graph locally

#     # For serverless user has to run `indexify deploy reference_graph.py` for the rest of the code to work.
#     # Open source clients would do `indexify deploy reference_graph.py`

#     # When working with a remote graph we can pull it from indexify
#     # We can add some wrapper function that generates the client and gets the remote graph.
#     tl = Client()
#     remote_summer = tl.get_graph("sequence_summer") # Same as summer.remote()
#     # or
#     remote_summer = indexify.get_graph("sequence_summer")

#     # indexify.ping() # Throw an exception if we can't communicate with the platform

#     # Maybe a future use case.
#     # map_result = summer.remove().invoke_node() # Invoke a specific function in the graph?

#     # The remote() method accepts optional credentials, if not provided we default to the indexify_API_KEY env var.
#     inv = summer.remote().queue(a=10) # This will invoke the graph remotely, non-blocking.

#     # If in non-blocking mode the user needs to poll inv.status() to check if the invocation is done.
#     inv_id = inv.id() # Stored for later use.

#     # Now is later use, we can add a module lever wrapper that creates a client and returns the invocation.
#     tl = Client() # Optionally pass creds or fetch from env.
#     inv = tl.get_invocation(inv_id)

#     inv.wait()
#     inv.raise_for_status() # This will raise an exception if the invocation failed?

#     # Insert some code to test the invocation status instead of using inv.raise_for_status()
#     print(inv.result()) # This will print the result of the invocation.
#     print(inv.logs()) # This will print the logs of the invocation.
#                       # Do we expose logs per function or just the whole invocation?
