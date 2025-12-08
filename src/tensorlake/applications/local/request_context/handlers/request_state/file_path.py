import base64
import os.path


def request_state_file_path(
    request_state_dir_path: str,
    state_key: str,
) -> str:
    # File system paths have restrictions on allowed characters.
    # Allow flexible characters in state_key by converting it into a base64 string.
    base64_state_key: str = base64.urlsafe_b64encode(state_key.encode("utf-8")).decode(
        "utf-8"
    )
    return os.path.join(
        request_state_dir_path,
        base64_state_key,
    )
