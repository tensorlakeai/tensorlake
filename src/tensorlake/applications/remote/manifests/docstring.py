import re
import textwrap
from enum import Enum


class DocstringStyle(Enum):
    GOOGLE = 1
    NUMPY = 2
    RESTRUCTURED_TEXT = 3
    UNKNOWN = 4


# reStructuredText (Sphinx).
# strict pattern: looks for :param x:, :return:, :raises:, etc.
_RST_DOCSTRING_DETECTION_HEURISTIC_PATTERN = re.compile(
    r"^\s*:(param|parameter|arg|argument|key|type|return|rtype|raise|raises|yield|yields|var|ivar|cvar)",
    re.MULTILINE,
)

# strict pattern: looks for a keyword followed strictly by a newline and a line of dashes.
_NUMPY_DOCSTRING_DETECTION_HEURISTIC_PATTERN = re.compile(
    r"^\s*(Parameters|Returns|Yields|Raises|See Also|Notes|References|Examples|Attributes|Methods)\s*\n\s*-{3,}\s*$",
    re.MULTILINE,
)

# strict pattern: looks for a keyword followed strictly by a colon and a newline.
# Note: 'Args' is the primary indicator for Google style.
_GOOGLE_DOCSTRING_DETECTION_HEURISTIC_PATTERN = re.compile(
    r"^\s*(Args|Arguments|Returns|Yields|Raises|Attributes|Methods):\s*$",
    re.MULTILINE,
)


def detect_docstring_style(docstring: str) -> DocstringStyle:
    """
    Detects if a docstring is Google-style, NumPy-style, or reStructuredText (Sphinx).

    Returns:
        DocStringStyle: The detected style enum member.

    Raises:
        Exception on internal errors.
    """
    if docstring == "":
        return DocstringStyle.UNKNOWN

    # Normalize indentation to make regex matching consistent
    docstring = textwrap.dedent(docstring)

    # --- Scoring ---

    rst_score = len(_RST_DOCSTRING_DETECTION_HEURISTIC_PATTERN.findall(docstring))
    # Weighted higher because headers are rare/distinct indicators
    numpy_score = (
        len(_NUMPY_DOCSTRING_DETECTION_HEURISTIC_PATTERN.findall(docstring)) * 2
    )
    google_score = (
        len(_GOOGLE_DOCSTRING_DETECTION_HEURISTIC_PATTERN.findall(docstring)) * 2
    )

    # --- Decision Logic ---

    # If no patterns match, it's likely a simple one-liner or unstructured text.
    if rst_score == 0 and numpy_score == 0 and google_score == 0:
        return DocstringStyle.UNKNOWN

    # Map scores to Enum members
    scores = {
        DocstringStyle.RESTRUCTURED_TEXT: rst_score,
        DocstringStyle.NUMPY: numpy_score,
        DocstringStyle.GOOGLE: google_score,
    }

    # Return the Enum member with the highest score
    return max(scores, key=scores.get)


def _join_lines(text_lines: list[str]) -> str:
    """Joins text lines into a single string removing extra whitespaces."""
    return " ".join([line.strip() for line in text_lines]).strip()


# Matches: ":param param_name: description"
_RST_PARAM_PATTERN: re.Pattern[str] = re.compile(
    r"^\s*:(?:param|parameter|arg|argument)\s+(\w+)\s*:\s*(.*)"
)
# Matches any fields like :return:, :raises:, etc.
_RST_ANY_FIELD_PATTERN: re.Pattern[str] = re.compile(r"^\s*:")


def _parse_rst_param_docstrings(lines: list[str]) -> dict[str, str]:
    """Parse parameter descriptions from reStructuredText (Sphinx) docstring.

    Args:
        docstring: The function's docstring, must be dedented already.

    Returns:
        Dictionary mapping parameter names to their descriptions

    Raises:
        Exception on parsing errors.
    """
    params: dict[str, str] = {}
    # Currently parsed parameter.
    current_param: str | None = None
    # Accumulator list for currently parsed parameter description lines.
    current_desc_lines: list[str] = []
    for line in lines:
        match: re.Match[str] | None = _RST_PARAM_PATTERN.match(line)
        if match is not None:
            if current_param is not None:
                params[current_param] = _join_lines(current_desc_lines)

            current_param = match.group(1)
            current_desc_lines = [match.group(2).strip()]

        elif current_param is not None:
            if _RST_ANY_FIELD_PATTERN.match(line):
                params[current_param] = _join_lines(current_desc_lines)
                current_param = None
                current_desc_lines = []
            else:
                if line.strip():
                    current_desc_lines.append(line.strip())

    if current_param is not None:
        params[current_param] = _join_lines(current_desc_lines)

    return params


# Matches: "param_name: description"
_GOOGLE_ARGUMENT_PATTERN: re.Pattern[str] = re.compile(
    r"^\s*(\*?\*?\w+)(?:\s*\(.*?\))?:\s*(.*)"
)


def _parse_google_param_docstrings(lines: list[str]) -> dict[str, str]:
    """Parses parameter descriptions from Google-style docstring.

    Args:
        docstring: The function's docstring, must be dedented already.
    Returns:
        Dictionary mapping parameter names to their descriptions.
    Raises:
        Exception on parsing errors.
    """
    params: dict[str, str] = {}
    extracting: bool = False
    current_param: str | None = None
    current_desc_lines: list[str] = []
    # Base indentation level for parameter descriptions. We have to track it to
    # know if a parameter description includes something like "Warning:" so we
    # don't detect "Warning:" as a new parameter.
    base_indent: int = 0

    for line in lines:
        stripped: str = line.strip()

        # 1. Detect Header
        if stripped in ("Args:", "Arguments:", "Parameters:"):
            extracting = True
            continue

        # 2. Detect End of Section (another header like 'Returns:')
        if extracting and stripped.endswith(":") and not stripped.startswith(".."):
            if current_param is not None:
                params[current_param] = _join_lines(current_desc_lines)
            break

        if extracting:
            if not stripped:
                continue

            # Calculate indentation level
            indent: int = len(line) - len(line.lstrip())

            # Establish baseline indentation for parameters
            if base_indent == 0:
                base_indent = indent
            # If we dedent below the baseline, we've left the Args block
            elif indent < base_indent:
                if current_param is not None:
                    params[current_param] = _join_lines(current_desc_lines)
                break

            match: re.Match[str] | None = _GOOGLE_ARGUMENT_PATTERN.match(line)

            # Check if this line is a new parameter definition
            if match is not None and indent == base_indent:
                # Save the previous parameter description
                if current_param:
                    params[current_param] = _join_lines(current_desc_lines)

                current_param = match.group(1)
                # group(2) is the text on the same line as the parameter name
                current_desc_lines = [match.group(2).strip()]

            # Otherwise, it's a continuation of the current description
            elif current_param:
                current_desc_lines.append(stripped)

    # Save the final parameter captured
    if current_param:
        params[current_param] = _join_lines(current_desc_lines)

    return params


# Matches: "param", "*args", "**kwargs", "x, y"
_NUMPY_PARAM_PATTERN: re.Pattern[str] = re.compile(
    r"^(\*{0,2}\w+(?:\s*,\s*\*{0,2}\w+)*)\s*(?::.*)?$"
)


def _parse_numpy_param_docstrings(lines: list[str]) -> dict[str, str]:
    """Parses parameter descriptions from NumPy-style docstring.

    Args:
        docstring: The function's docstring
    Returns:
        Dictionary mapping parameter names to their descriptions
    Raises:
        Exception on parsing errors.
    """
    params: dict[str, str] = {}
    extracting: bool = False
    current_params: list[str] = []
    current_desc_lines: list[str] = []

    for i, line in enumerate(lines):
        stripped: str = line.strip()

        # 1. Header Detection
        if stripped == "Parameters":
            # Look ahead to confirm NumPy style header (dashed line underneath)
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                extracting = True
                continue

        if extracting:
            # Skip the dashed line itself
            if stripped.startswith("---"):
                continue

            # 2. Stop Condition: Another Header
            # If we see a new header (Line followed by dashes), we stop.
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                if current_params:
                    desc: str = _join_lines(current_desc_lines)
                    for p in current_params:
                        params[p] = desc
                break

            if not stripped:
                continue

            # 3. Parse Definitions vs Descriptions
            match: re.Match[str] | None = _NUMPY_PARAM_PATTERN.match(stripped)
            indent: int = len(line) - len(line.lstrip())

            # NumPy Strictness: Definitions must be at base indentation (0)
            if indent == 0 and match:
                # Save previous parameters
                if current_params:
                    desc: str = _join_lines(current_desc_lines)
                    for p in current_params:
                        params[p] = desc

                # Start new parameters
                raw_names: str = match.group(1)
                current_params = [n.strip() for n in raw_names.split(",")]
                current_desc_lines = []

            # If indented, or no match found at indent 0, treat as description
            elif current_params:
                current_desc_lines.append(stripped)

    # Save final batch
    if current_params:
        desc: str = _join_lines(current_desc_lines)
        for p in current_params:
            params[p] = desc

    return params


def parameter_docstrings(
    docstring: str, docstring_style: DocstringStyle
) -> dict[str, str]:
    """Parse parameter descriptions from docstring.

    Supports Google-style, NumPy-style, and simple parameter descriptions.

    Args:
        docstring: The function's docstring
        docstring_style: The format of the docstring

    Returns:
        Dictionary mapping parameter names to their descriptions

    Raises:
        Exception on parsing errors.
    """
    if len(docstring) == 0:
        return {}

    lines: list[str] = textwrap.dedent(docstring).splitlines()

    if docstring_style == DocstringStyle.RESTRUCTURED_TEXT:
        return _parse_rst_param_docstrings(lines)
    elif docstring_style == DocstringStyle.GOOGLE:
        return _parse_google_param_docstrings(lines)
    elif docstring_style == DocstringStyle.NUMPY:
        return _parse_numpy_param_docstrings(lines)
    else:
        return {}


# Matches reStructuredText return fields: :return: or :returns:
_RST_RETURN_PATTERN: re.Pattern[str] = re.compile(r":returns?:(.*)")


def _parse_rst_return_docstring(lines: list[str]) -> str | None:
    """Parses return description from reStructuredText docstrings.

    The lines should be dedented already.
    """
    for i, line in enumerate(lines):
        match: re.Match[str] | None = _RST_RETURN_PATTERN.search(line)
        if match:
            content: list[str] = [match.group(1).strip()]
            current_indent: int = len(line) - len(line.lstrip())

            for next_line in lines[i + 1 :]:
                if not next_line.strip():
                    continue

                next_indent: int = len(next_line) - len(next_line.lstrip())
                if next_indent > current_indent:
                    content.append(next_line.strip())
                # Stop if we hit another field (e.g., :rtype:)
                elif next_line.strip().startswith(":"):
                    break
                else:
                    break

            result = _join_lines(content)
            return result if result else None
    return None


def _parse_google_return_docstring(lines: list[str]) -> str | None:
    """Parses return description from Google-style docstrings.

    The lines should be dedented already."""
    extracting: bool = False
    content: list[str] = []
    base_indent: int = 0

    for line in lines:
        stripped: str = line.strip()

        if stripped == "Returns:":
            extracting = True
            continue

        # Stop if we hit another section (like Raises: or Yields:)
        if extracting and stripped.endswith(":") and not stripped.startswith(".."):
            break

        if extracting:
            if not stripped:
                continue

            indent: int = len(line) - len(line.lstrip())

            # Establish baseline indent for the block
            if not content:
                base_indent = indent

            # If we dedent, we have left the Returns block
            if indent < base_indent:
                break

            # Google Style often puts type on the first line: "int: Description"
            if not content:
                if ":" in stripped:
                    parts: list[str] = stripped.split(":", 1)
                    # Heuristic: If left side is short (<20 chars), it's likely a type.
                    if len(parts[0]) < 20:
                        content.append(parts[1].strip())
                    else:
                        content.append(stripped)
                else:
                    content.append(stripped)
            else:
                content.append(stripped)

    if not content:
        return None

    return _join_lines(content)


def _parse_numpy_return_docstring(lines: list[str]) -> str | None:
    """Parses return description from NumPy-style docstrings."""
    extracting: bool = False
    skip_next: bool = False
    content: list[str] = []

    for i, line in enumerate(lines):
        stripped: str = line.strip()

        # Header detection
        if stripped == "Returns":
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                extracting = True
                skip_next = True  # Skip the dashes
                continue

        if extracting:
            if stripped.startswith("---"):
                continue

            # In strict NumPy, the line after dashes is the Type.
            # We skip it to capture the description indented below.
            if skip_next:
                skip_next = False
                continue

            # Stop at the next section header (line followed by dashes)
            if i + 1 < len(lines) and lines[i + 1].strip().startswith("---"):
                break

            if not stripped:
                continue

            content.append(stripped)

    if not content:
        return None

    return _join_lines(content)


def return_value_description(
    docstring: str, docstring_style: DocstringStyle
) -> str | None:
    """
    Extracts the description of the return value from a docstring, removing types if present.

    Args:
        docstring (str): The raw docstring.
        docstring_style (_DocStringStyle): The format of the docstring.

    Returns:
        str | None: The text description of the return value, or None if not found.

    Raises:
        Exception on parsing errors.
    """
    if docstring == "":
        return None

    lines: list[str] = textwrap.dedent(docstring).splitlines()
    if docstring_style == DocstringStyle.RESTRUCTURED_TEXT:
        return _parse_rst_return_docstring(lines)
    elif docstring_style == DocstringStyle.GOOGLE:
        return _parse_google_return_docstring(lines)
    elif docstring_style == DocstringStyle.NUMPY:
        return _parse_numpy_return_docstring(lines)
    else:
        return None
