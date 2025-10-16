#!/usr/bin/env python3
"""Main entry point for tensorlake-python-analyzer."""

import argparse
import json
import sys
import traceback

from .core import analyze_code


def create_parser() -> argparse.ArgumentParser:
    """Create command line argument parser."""
    parser = argparse.ArgumentParser(
        prog="tensorlake-python-analyzer",
        description="Analyzes Python applications and outputs metadata in JSON format",
        epilog="""
Example:
    tensorlake-python-analyzer myapp.py
    tensorlake-python-analyzer --pretty myapp.py
    tensorlake-python-analyzer myapp.py -o output.json --pretty
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "application_file_path",
        metavar="APPLICATION-FILE-PATH",
        type=str,
        help="Path to the Python file containing Tensorlake applications",
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        default=None,
        help="Output file path. If not provided, outputs to stdout.",
    )

    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print the JSON output with indentation.",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    return parser


def main():
    """Main function for the analyzer CLI."""
    parser = create_parser()
    args = parser.parse_args()

    # Print status message to stderr so it doesn't interfere with JSON output
    print(
        f"Analyzing applications from {args.application_file_path}",
        file=sys.stderr,
    )

    try:
        # Analyze the code
        analysis_result = analyze_code(args.application_file_path)

        # Convert to dictionary
        result_dict = analysis_result.to_dict()

        # Convert to JSON
        if args.pretty:
            json_output = json.dumps(result_dict, indent=2)
        else:
            json_output = json.dumps(result_dict)

        # Output to file or stdout
        if args.output:
            with open(args.output, "w") as f:
                f.write(json_output)
            print(
                f"\033[92mAnalysis complete. Output written to {args.output}\033[0m",
                file=sys.stderr,
            )
        else:
            # Output to stdout (not stderr)
            print(json_output)
            print("\033[92mAnalysis complete.\033[0m", file=sys.stderr)

    except Exception as e:
        print(
            f"\033[91mFailed to analyze the application file: {e}\033[0m",
            file=sys.stderr,
        )
        traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
