import os
import urllib.request
from typing import List, Optional

import click  # Used for pretty printing to console.

from tensorlake import Graph, RemoteGraph, tensorlake_function

# Define a function for each workflow step


# 1. Generate a sequence of numbers from 0 to the supplied value.
@tensorlake_function()
def generate_sequence(last_sequence_number: int) -> List[int]:
    # This function impelements a map operation because it returns a list.
    return [i for i in range(last_sequence_number + 1)]


# 2. Compute square of each number.
@tensorlake_function()
def squared(number: int) -> int:
    # This function transforms each element of the sequence because it accepts
    # only a single int as a parameter.
    return number * number


# 3. Sum all the squares.
@tensorlake_function(accumulate=int)
def sum_all(current_sum: int, number: int) -> int:
    # This function implements a reduce operation.
    # It is called for each element of the sequence. The returned value is passed
    # to the next call in `current_sum` parameter. The first call gets `current_sum`=int()
    # which is 0. The return value of the last call is the result of the reduce operation.
    return current_sum + number


# 4. Send the sum to a web service.
@tensorlake_function()
def send_to_web_service(value: int) -> str:
    # This function accepts the sum from the previous step and sends it to a web service.
    url = f"https://example.com/?number={value}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req) as response:
        return response.read()


g = Graph(
    name="example_workflow",
    start_node=generate_sequence,
    description="Example workflow",
)
g.add_edge(generate_sequence, squared)
g.add_edge(squared, sum_all)
g.add_edge(sum_all, send_to_web_service)


# Invoke the workflow for sequence [0..200].
def run_workflow(g: Graph) -> None:
    invocation_id: str = g.run(last_sequence_number=200, block_until_done=True)

    # Get the output of the the workflow (of its last step).
    last_step_output: str = g.output(invocation_id, "send_to_web_service")
    click.secho("Web service response:", fg="green", bold=True)
    click.echo(last_step_output[0])
    click.echo()

    # Get the sum.
    sum_output: str = g.output(invocation_id, "sum_all")
    click.secho("Sum:", fg="green", bold=True)
    click.echo(sum_output[0])
    click.echo()


def fetch_workflow_from_cloud() -> Optional[RemoteGraph]:
    try:
        return RemoteGraph.by_name("example_workflow")
    except Exception:
        click.secho(
            f"Could not fetch the workflow 'example_workflow' from Tensorlake Cloud.",
            fg="red",
            bold=True,
        )
        click.secho(
            "Did you deploy the workflow to Tensorlake Cloud?",
            fg="green",
            bold=True,
        )


if __name__ == "__main__":
    # Create the workflow on your computer and run it locally.
    run_workflow(g)

    # Fetch the workflow from the cloud and run it on cloud.
    if "TENSORLAKE_API_KEY" in os.environ:
        cloud_workflow: Optional[RemoteGraph] = fetch_workflow_from_cloud()
        if cloud_workflow is not None:
            run_workflow(cloud_workflow)
    else:
        click.secho(
            "Skipping running the workflow on the cloud because TENSORLAKE_API_KEY environment variable is not defined.",
            fg="yellow",
        )
        click.secho("Did you register at Tensorlake Cloud?", fg="green", bold=True)
