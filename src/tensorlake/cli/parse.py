import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.models import ParsingOptions
from tensorlake.documentai.models._enums import ChunkingStrategy, TableOutputMode
from tensorlake.utils.cache import KVCache


def _emit(obj):
    print(json.dumps(obj), flush=True)


def parse(path_or_url: str, pages: str | None, ignore_cache: bool):
    """Parse a document and emit NDJSON events to stdout."""
    api_key = os.environ.get("TENSORLAKE_API_KEY") or os.environ.get("TENSORLAKE_PAT")
    api_url = os.environ.get("TENSORLAKE_API_URL", "https://api.tensorlake.ai")

    client = DocumentAI(api_key=api_key, server_url=api_url)

    page_key = pages.strip() if pages else "all"
    is_url = path_or_url.startswith(("http://", "https://"))

    source = path_or_url
    cache_identity = None
    if not is_url:
        p = Path(path_or_url)
        if not p.exists() or not p.is_file():
            _emit({"type": "error", "message": f"File not found: {path_or_url}"})
            sys.exit(1)

        _emit({"type": "status", "message": f"Uploading {p.name}..."})

        hasher = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
        cache_identity = f"file:{hasher.hexdigest()}"

        file_id = client.upload(str(p))
        source = file_id
    else:
        cache_identity = f"url:{path_or_url}"

    cache = KVCache("parse")
    cache_key = f"{cache_identity}|pages:{page_key}"
    if not ignore_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            _emit({"type": "cached", "hit": True})
            _emit({"type": "output", "content": cached})
            return

    _emit({"type": "status", "message": "Parsing..."})

    options = ParsingOptions(
        chunking_strategy=ChunkingStrategy.FRAGMENT,
        table_output_mode=TableOutputMode.MARKDOWN,
    )

    result = client.parse_and_wait(
        file=source, parsing_options=options, page_range=pages
    )

    if not result.chunks:
        _emit({"type": "output", "content": ""})
        return

    markdown = "\n\n".join(chunk.content for chunk in result.chunks)

    cache.set(cache_key, markdown)

    _emit({"type": "output", "content": markdown})


def parse_entrypoint():
    """Entry point for the parse command (called from Rust CLI via python -m)."""
    parser = argparse.ArgumentParser(description="Parse a document and output markdown")
    parser.add_argument("path_or_url", help="Local file path or HTTP URL")
    parser.add_argument(
        "--pages",
        default=None,
        help="Pages to parse, e.g. '1', '1-5', or '1,2,10'. Default: all pages.",
    )
    parser.add_argument(
        "--ignore-cache",
        action="store_true",
        default=False,
        help="Ignore local cache and reparse the document.",
    )
    args = parser.parse_args()

    try:
        parse(
            path_or_url=args.path_or_url,
            pages=args.pages,
            ignore_cache=args.ignore_cache,
        )
    except SystemExit:
        raise
    except Exception as e:
        _emit({"type": "error", "message": f"{type(e).__name__}: {e}"})
        sys.exit(1)


if __name__ == "__main__":
    parse_entrypoint()
