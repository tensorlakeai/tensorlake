import re

from tensorlake.applications import DeserializationError
from tensorlake.applications.function.application_call import (
    SerializedApplicationArgument,
)

# NB: Only use native bytes.find(...) methods for string search in bytes because manual
# iteration in Python is 1000x and more slower than optimized C implementations of .find(...).


def parse_application_function_call_args_from_http_request(
    http_request: bytes,
) -> tuple[
    list[SerializedApplicationArgument], dict[str, SerializedApplicationArgument]
]:
    """Parse HTTP request bytes to extract serialized application function call arguments.

    No data copying is performed; memoryviews are used to reference the original bytes.
    Returns args, kwargs as lists/dicts of SerializedApplicationArgument.
    Raises DeserializationError if parsing fails.
    """
    # Find the end of the request line (first line)
    request_line_end: int = http_request.find(b"\r\n")
    if request_line_end == -1:
        raise DeserializationError("Malformed HTTP request: no request line found")

    headers_start: int = request_line_end + 2  # Skip the \r\n
    headers: dict[str, str]
    body_offset: int
    headers, body_offset = _parse_lowercase_http_request_headers(
        buffer=http_request, headers_start=headers_start
    )

    # Don't use the original content-length header because it's not always equal to actual body size.
    # - Content-Length is not used with chunked transfer encoding.
    # - HTTP/2 and HTTP/3 don't use Content-Length header.
    if len(http_request) == body_offset:
        return [], {}  # Application function call with no arguments (empty body).

    if "content-type" not in headers:
        return [], {}  # Application function call with no arguments (empty body).

    content_type: str = headers["content-type"]
    if content_type.startswith("multipart/form-data"):
        return parse_application_function_call_args_from_multipart_form_data(
            body_buffer=http_request, body_offset=body_offset, content_type=content_type
        )
    else:
        arg: SerializedApplicationArgument = (
            parse_application_function_call_arg_from_single_payload(
                body_buffer=http_request,
                body_offset=body_offset,
                body_end_offset=len(http_request),
                content_type=content_type,
            )
        )
        # Single payload is always mapped to the first positional application function argument.
        return [arg], {}


def parse_application_function_call_args_from_multipart_form_data(
    body_buffer: bytes,
    body_offset: int,
    content_type: str,
) -> tuple[
    list[SerializedApplicationArgument], dict[str, SerializedApplicationArgument]
]:
    """Parse multipart/form-data body to extract serialized application function call arguments.

    No data copying is performed; memoryviews are used to reference the original bytes.
    Returns args, kwargs as lists/dicts of SerializedApplicationArgument.
    Raises DeserializationError if parsing fails.
    """
    serialized_args_mapping: dict[int, SerializedApplicationArgument] = {}
    serialized_kwargs: dict[str, SerializedApplicationArgument] = {}

    # Extract the boundary from the content_type
    boundary_prefix: str = "boundary="

    boundary_start: int = content_type.find(boundary_prefix)
    if boundary_start == -1:
        raise DeserializationError(
            f"Missing or malformed boundary in Content-Type header: {content_type}"
        )

    boundary: bytes = (
        content_type[boundary_start + len(boundary_prefix) :]
        .encode("utf-8")
        .strip(b'"')
    )  # Remove optional quotes around boundary value
    if len(boundary) == 0:
        raise DeserializationError("Boundary is empty")

    # Define the boundary markers
    part_start_sequence: bytes = b"--" + boundary
    part_body_end_sequence: bytes = b"\r\n" + part_start_sequence
    body_end_sequence: bytes = b"--" + boundary + b"--\r\n"

    next_part_start_sequence_offset: int = body_offset
    while True:
        part_start_sequence_offset: int = body_buffer.find(
            part_start_sequence, next_part_start_sequence_offset
        )
        if part_start_sequence_offset == -1:
            break  # Abrupt end of multipart body, should not happen in well-formed requests.

        body_end_sequence_offset: int = body_buffer.find(
            body_end_sequence,
            next_part_start_sequence_offset,
            next_part_start_sequence_offset + len(body_end_sequence),
        )
        if part_start_sequence_offset == body_end_sequence_offset:
            # Reached the final body boundary, no more parts after it.
            # This is --boundary--\r\n
            break

        part_headers_offset: int = (
            part_start_sequence_offset + len(part_start_sequence) + 2
        )  # Skip \r\n
        part_headers: dict[str, str]
        part_body_offset: int
        part_headers, part_body_offset = _parse_lowercase_http_request_headers(
            buffer=body_buffer,
            headers_start=part_headers_offset,
        )
        part_body_end_offset: int = body_buffer.find(
            part_body_end_sequence, part_body_offset
        )
        if part_body_end_offset == -1:
            raise DeserializationError(
                "Malformed multipart body: missing part body closing boundary"
            )
        # Points at part_start_sequence of the next part.
        next_part_start_sequence_offset = part_body_end_offset + 2

        # Use application/octet-stream as default content type.
        part_content_type: str = part_headers.get(
            "content-type", "application/octet-stream"
        )

        # Extract the field name from Content-Disposition
        if "content-disposition" not in part_headers:
            raise DeserializationError(
                f"Missing Content-Disposition header in part headers: {part_headers}"
            )
        content_disposition: str = part_headers["content-disposition"]

        match: re.Match | None = _CONTENT_DISPOSITION_REGEX.search(content_disposition)
        if match is None:
            continue  # Skip parts without valid field name. This is what web frameworks typically do.

        part_field_name: str = match.group(1)
        part_arg: SerializedApplicationArgument = (
            parse_application_function_call_arg_from_single_payload(
                body_buffer=body_buffer,
                body_offset=part_body_offset,
                body_end_offset=part_body_end_offset,
                content_type=part_content_type,
            )
        )

        try:
            part_field_index: int = int(part_field_name)
            # field name is int -> positional argument
            serialized_args_mapping[part_field_index] = part_arg
        except ValueError:
            # field name is an identifier -> keyword argument
            serialized_kwargs[part_field_name] = part_arg

    # Convert the mapping to a list for positional arguments without gaps in indexes.
    # Allow indexing starting from 0 or 1.
    serialized_args: list[SerializedApplicationArgument] = []
    first_index: int = 0 if 0 in serialized_args_mapping else 1
    for index in range(first_index, first_index + len(serialized_args_mapping)):
        if index not in serialized_args_mapping:
            raise DeserializationError(
                f"Missing positional argument at index {index} in multipart body"
            )
        serialized_args.append(serialized_args_mapping[index])

    return serialized_args, serialized_kwargs


def parse_application_function_call_arg_from_single_payload(
    body_buffer: bytes, body_offset: int, body_end_offset: int, content_type: str
) -> SerializedApplicationArgument:
    """Parse single payload body to extract serialized application function call argument.

    No data copying is performed; memoryviews are used to reference the original bytes.
    Returns SerializedApplicationArgument of the arg.
    Raises DeserializationError if parsing fails.
    """
    serialized_arg = SerializedApplicationArgument(
        data=memoryview(body_buffer)[body_offset:body_end_offset],
        content_type=content_type,
    )
    return serialized_arg


def _parse_lowercase_http_request_headers(
    buffer: bytes, headers_start: int
) -> tuple[dict[str, str], int]:
    """
    Parse HTTP/1.1 request headers into a dictionary.

    Returns:
        dict[str, str]: A dictionary containing the HTTP headers as key-value pairs.
                        Header names are converted to lowercase. This is important
                        because HTTP header names use different casing conventions
                        depending on HTTP version and client implementations.
        int: offset where body starts in the http_request.

    Raises:
        DeserializationError: If failed to parse the request.
    """
    headers: dict[str, str] = {}

    header_read_offset: int = headers_start
    while header_read_offset < len(buffer):
        # Find the end of the current header line
        header_end: int = buffer.find(b"\r\n", header_read_offset)
        if header_end == -1:
            raise DeserializationError(
                "Malformed HTTP request: headers not properly terminated"
            )

        # Stop if we encounter an empty line (end of headers)
        if header_read_offset == header_end:
            # Body starts after the empty line
            return headers, header_end + 2

        # Extract the header line
        try:
            header_line: str = buffer[header_read_offset:header_end].decode("utf-8")
        except UnicodeDecodeError as e:
            raise DeserializationError(f"Malformed HTTP header: {e}")

        header_line_split: list[str] = header_line.split(sep=":", maxsplit=1)
        if len(header_line_split) != 2:
            raise DeserializationError("Malformed HTTP header: missing colon separator")

        key: str = header_line_split[0]
        value: str = header_line_split[1].lstrip()  # Remove leading spaces from value
        headers[key.lower()] = value  # Imporant: lowercase header names

        # Move to the next header line
        header_read_offset = header_end + 2

    # If no empty line is found, assume no body
    return headers, len(buffer)


# Pre-compile regex for extracting field name from Content-Disposition
_CONTENT_DISPOSITION_REGEX: re.Pattern = re.compile(r'form-data;\s*name="([^"]+)"')
