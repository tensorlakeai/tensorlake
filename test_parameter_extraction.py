#!/usr/bin/env python3

import sys

sys.path.insert(0, "src")

from typing import List

from tensorlake.functions_sdk import Graph, tensorlake_function


@tensorlake_function()
def process_text(input_text: str, max_length: int = 100) -> str:
    """Process text with optional max length.

    Args:
        input_text: The text string to process
        max_length: Maximum number of characters to keep
    """
    return input_text[:max_length]


@tensorlake_function()
def count_words(text: str) -> int:
    """Count words in text.

    Args:
        text: The input text to count words in
    """
    return len(text.split())


@tensorlake_function()
def generate_numbers(count: int) -> List[int]:
    """Generate a list of numbers.

    Args:
        count: How many numbers to generate
    """
    return list(range(count))


def test_parameter_extraction():
    graph = Graph(name="test_graph", start_node=process_text)
    graph.add_edge(process_text, count_words)
    graph.add_edge(count_words, generate_numbers)

    metadata = graph.definition()

    print("=== Start Node Parameters ===")
    start_node = metadata.entrypoint
    print(f"Function: {start_node.name}")
    print(f"Description: {start_node.description}")
    if start_node.parameters:
        for param in start_node.parameters:
            default_val = param.data_type.get("default", "None")
            print(
                f"  {param.name}: {param.data_type} (required: {param.required}, default: {default_val}, description: {param.description})"
            )
    print(f"Return type: {start_node.return_type}")
    print()

    print("=== All Function Parameters ===")
    for func_name, func_meta in metadata.functions.items():
        print(f"Function: {func_name}")
        print(f"Description: {func_meta.description}")
        if func_meta.parameters:
            for param in func_meta.parameters:
                default_val = param.data_type.get("default", "None")
                print(
                    f"  {param.name}: {param.data_type} (required: {param.required}, default: {default_val}, description: {param.description})"
                )
        print(f"Return type: {func_meta.return_type}")
        print()


if __name__ == "__main__":
    test_parameter_extraction()
