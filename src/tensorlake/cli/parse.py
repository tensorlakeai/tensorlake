import hashlib
from pathlib import Path
from typing import Optional

import click

from tensorlake.cli._common import Context, pass_auth
from tensorlake.documentai.client import DocumentAI
from tensorlake.documentai.models import ParsingOptions
from tensorlake.documentai.models._enums import ChunkingStrategy, TableOutputMode
from tensorlake.utils.cache import KVCache


@click.command()
@click.argument("path_or_url", required=True)
@click.option(
    "--pages",
    "pages",
    default=None,
    help="Pages to parse, e.g. '1', '1-5', or '1,2,10'. Default: all pages.",
)
@click.option(
    "--ignore-cache",
    is_flag=True,
    default=False,
    help="Ignore local cache (~/.tensorlake/cache/parse) and reparse the document.",
)
@pass_auth
def parse(ctx: Context, path_or_url: str, pages: Optional[str], ignore_cache: bool):
    """
    Parse a local document path or HTTP URL and print markdown to stdout.
    """
    client = DocumentAI(api_key=ctx.api_key, server_url=ctx.api_url)

    # Build cache key: file hash (for local files) or URL + page range
    page_key = pages.strip() if pages else "all"
    is_url = path_or_url.startswith(("http://", "https://"))

    source = path_or_url
    cache_identity = None
    if not is_url:
        p = Path(path_or_url)
        if not p.exists() or not p.is_file():
            raise click.UsageError(f"File not found: {path_or_url}")

        # Compute content hash for stable cache identity
        hasher = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                hasher.update(chunk)
        cache_identity = f"file:{hasher.hexdigest()}"

        file_id = client.upload(str(p))
        source = file_id
    else:
        cache_identity = f"url:{path_or_url}"

    # Cache lookup
    cache = KVCache("parse")
    cache_key = f"{cache_identity}|pages:{page_key}"
    if not ignore_cache:
        cached = cache.get(cache_key)
        if cached is not None:
            click.echo(cached)
            return

    options = ParsingOptions(
        chunking_strategy=ChunkingStrategy.FRAGMENT,
        table_output_mode=TableOutputMode.MARKDOWN,
    )

    result = client.parse_and_wait(
        file=source, parsing_options=options, page_range=pages
    )

    if not result.chunks:
        click.echo("")
        return

    markdown = "\n\n".join(chunk.content for chunk in result.chunks)

    # Save to cache for future runs
    cache.set(cache_key, markdown)

    click.echo(markdown)
