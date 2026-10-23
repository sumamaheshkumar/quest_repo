"""Shared HTTP configuration for source ingestion."""

import logging
import os
import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


def build_session(config: dict) -> requests.Session:
    """Create a reusable session with contact information and retries."""
    email_variable = config["contact_email_env"]
    email = os.environ.get(email_variable, "").strip()

    if not email or "@" not in email or "\n" in email or "\r" in email:
        raise ValueError(
            f"Set {email_variable} to a valid contact email before ingestion."
        )

    settings = config["http"]

    retries = Retry(
        total=settings["max_retries"],
        connect=settings["max_retries"],
        read=settings["max_retries"],
        status=settings["max_retries"],
        other=0,
        allowed_methods=frozenset({"GET", "HEAD"}),
        status_forcelist=(429, 500, 502, 503, 504),
        backoff_factor=settings["backoff_factor"],
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": f"RearcDataQuest/0.1 (contact: {email})",
        }
    )
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=retries,
            pool_connections=2,
            pool_maxsize=2,
        ),
    )
    return session


def get_response(
    session: requests.Session,
    url: str,
    config: dict,
) -> requests.Response:
    """Fetch a source response and reject unsuccessful HTTP statuses."""
    settings = config["http"]
    started = time.monotonic()

    try:
        response = session.get(
            url,
            timeout=(
                settings["connect_timeout_seconds"],
                settings["read_timeout_seconds"],
            ),
        )
    except requests.RequestException:
        LOGGER.exception(
            "source_request_failed elapsed_seconds=%.3f",
            time.monotonic() - started,
        )
        raise

    LOGGER.info(
        "source_request_finished status=%s bytes=%s elapsed_seconds=%.3f",
        response.status_code,
        len(response.content),
        time.monotonic() - started,
    )

    try:
        response.raise_for_status()
    except requests.HTTPError:
        response.close()
        raise

    return response