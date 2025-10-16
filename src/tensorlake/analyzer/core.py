"""Core analysis functionality."""

import os

from tensorlake.applications.remote.code.loader import load_code

from .converter import extract_functions_and_applications, extract_images
from .models import AnalysisOutput


def analyze_code(application_file_path: str) -> AnalysisOutput:
    """Analyze Python code and extract image, function, and application information.

    Args:
        application_file_path: Path to the Python file containing Tensorlake applications

    Returns:
        AnalysisOutput containing images, functions, applications, and code manifest

    Raises:
        Exception: If the file cannot be loaded or analyzed
    """
    # Load the code
    application_file_path = os.path.abspath(application_file_path)
    code_dir_path = os.path.dirname(application_file_path)
    load_code(application_file_path)

    # Extract images
    images_dict = extract_images()

    # Extract functions, applications, and code manifest
    functions_dict, applications_dict, code_manifest = (
        extract_functions_and_applications(code_dir_path)
    )

    return AnalysisOutput(
        images=images_dict,
        functions=functions_dict,
        applications=applications_dict,
        code_manifest=code_manifest,
    )
