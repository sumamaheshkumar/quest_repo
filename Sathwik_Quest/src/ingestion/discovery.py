"""Discover files directly inside the configured BLS directory."""

from html.parser import HTMLParser
from urllib.parse import unquote, urljoin, urlsplit

import requests

from ingestion.http_client import get_response


class DirectoryLinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return

        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)


def parse_directory(html: str, directory_url: str) -> dict[str, str]:
    """Return filename-to-URL mappings for direct files only."""
    base = urlsplit(directory_url)

    if base.scheme != "https" or not base.path.endswith("/"):
        raise ValueError("Expected an HTTPS directory URL ending in '/'.")

    parser = DirectoryLinkParser()
    parser.feed(html)
    parser.close()

    files = {}

    for href in parser.links:
        absolute_url = urljoin(directory_url, href)
        candidate = urlsplit(absolute_url)

        # Ignore other websites, query links, and page anchors.
        if (
            candidate.scheme != base.scheme
            or candidate.netloc != base.netloc
            or candidate.query
            or candidate.fragment
        ):
            continue

        if not candidate.path.startswith(base.path):
            continue

        relative_path = candidate.path[len(base.path):]
        filename = unquote(relative_path)

        # Accept direct files, excluding directories and unsafe names.
        if (
            not filename
            or filename in {".", ".."}
            or "/" in filename
            or "\\" in filename
            or any(ord(character) < 32 for character in filename)
        ):
            continue

        if filename in files and files[filename] != absolute_url:
            raise ValueError(f"Conflicting URLs for file: {filename}")

        files[filename] = absolute_url

    if not files:
        raise ValueError(
            "No BLS files discovered. Stop ingestion; "
            "do not interpret an empty listing as source deletions."
        )

    return dict(sorted(files.items()))


def discover_bls_files(
    session: requests.Session,
    config: dict,
) -> dict[str, str]:
    directory_url = config["bls_directory_url"]

    with get_response(session, directory_url, config) as response:
        return parse_directory(response.text, directory_url)