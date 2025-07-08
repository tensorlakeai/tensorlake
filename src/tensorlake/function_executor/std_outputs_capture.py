# Common code for capturing function standard outputs

import io


def flush_logs(stdout_file: io.StringIO, stderr_file: io.StringIO) -> None:
    print("", end="", flush=True)
    stdout_file.flush()
    stderr_file.flush()


def read_till_the_end(file: io.StringIO, start: int) -> str:
    end: int = file.tell()
    file.seek(start)
    text: str = file.read(end - start)
    file.seek(0, io.SEEK_END)  # Move the position back to the end
    return text
