"""Download and validate a source snapshot before publication."""

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from ingestion.discovery import discover_bls_files
from ingestion.http_client import build_session, get_response

LOGGER = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def validate_population(payload):
    """Reject incomplete, malformed, or ambiguous population results."""
    if not isinstance(payload, dict):
        raise ValueError("Population response must be a JSON object.")

    records = payload.get("data")
    if not isinstance(records, list) or not records:
        raise ValueError("Population data must be a nonempty list.")

    page = payload.get("page")
    if not isinstance(page, dict):
        raise ValueError("Population pagination metadata is missing.")

    if page.get("offset") != 0 or page.get("total") != len(records):
        raise ValueError(
            "Population response is incomplete; pagination requires handling."
        )

    required = {"Nation ID", "Nation", "Year", "Population"}
    seen = set()

    for row in records:
        if not isinstance(row, dict) or not required.issubset(row):
            raise ValueError("Population record is missing required fields.")

        if (
            row["Nation ID"] != "01000US"
            or row["Nation"] != "United States"
        ):
            raise ValueError("Unexpected nation in population response.")

        year = row["Year"]
        if type(year) is not int or not 1900 <= year <= 2100:
            raise ValueError(f"Invalid population year: {year!r}")

        population = row["Population"]
        if (
            type(population) not in (int, float)
            or not math.isfinite(population)
            or population <= 0
            or int(population) != population
        ):
            raise ValueError(f"Invalid population value for {year}.")

        key = (row["Nation ID"], year)
        if key in seen:
            raise ValueError(f"Duplicate population key: {key}")
        seen.add(key)

    required_years = set(range(2013, 2019))
    available_years = {row["Year"] for row in records}
    if missing := required_years - available_years:
        raise ValueError(f"Assessment population years missing: {sorted(missing)}")

    return len(records)


def save_file(snapshot_dir, relative_path, content, source_url):
    """Save exact response bytes and describe them in a manifest entry."""
    destination = snapshot_dir / relative_path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)

    return {
        "relative_path": relative_path,
        "source_url": source_url,
        "retrieved_at_utc": utc_now(),
        "size_bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def download_snapshot(config):
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "_"
        + uuid4().hex
    )
    snapshot_dir = Path(".local/snapshots") / run_id
    snapshot_dir.mkdir(parents=True, exist_ok=False)
    started_at = utc_now()
    entries = []

    with build_session(config) as session:
        inventory = discover_bls_files(session, config)

        required_bls = {"pr.data.1.AllData", "pr.series", "pr.txt"}
        if missing := required_bls - set(inventory):
            raise ValueError(f"Required BLS files missing: {sorted(missing)}")

        for filename, url in inventory.items():
            with get_response(session, url, config) as response:
                if response.status_code != 200:
                    raise ValueError(f"Unexpected status for {filename}")

                content = response.content
                content_type = response.headers.get("Content-Type", "").lower()
                prefix = content.lstrip()[:256].lower()

                if (
                    not content.strip()
                    or "text/html" in content_type
                    or prefix.startswith((b"<!doctype html", b"<html"))
                ):
                    raise ValueError(f"Empty or HTML response for {filename}")

                entries.append(
                    save_file(snapshot_dir, f"bls/{filename}", content, url)
                )
                LOGGER.info("download_validated file=%s", filename)

        url = config["population_url"]
        with get_response(session, url, config) as response:
            if response.status_code != 200:
                raise ValueError("Unexpected population HTTP status.")

            population_count = validate_population(response.json())
            entries.append(
                save_file(
                    snapshot_dir,
                    "population/population.json",
                    response.content,
                    url,
                )
            )

    manifest = {
        "manifest_version": 1,
        "run_id": run_id,
        "status": "download_validated",
        "started_at_utc": started_at,
        "completed_at_utc": utc_now(),
        "bls_file_count": len(inventory),
        "population_record_count": population_count,
        "files": entries,
    }

    # A failed run leaves no completed manifest.
    temporary_manifest = snapshot_dir / "manifest.json.tmp"
    temporary_manifest.write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary_manifest.replace(snapshot_dir / "manifest.json")

    return snapshot_dir


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )
    config = json.loads(
        Path("config/ingestion.json").read_text(encoding="utf-8")
    )
    snapshot_dir = download_snapshot(config)
    print(f"\nPASS: Download snapshot validated: {snapshot_dir}")


if __name__ == "__main__":
    main()