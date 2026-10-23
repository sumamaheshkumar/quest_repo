import os
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

import requests


BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POPULATION_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year%2CNation"
    "&locale=en"
    "&measures=Population"
)


class DirectoryParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)


def check_bls(contact_email):
    response = requests.get(
        BLS_URL,
        headers={
            "User-Agent": (
                f"RearcDataQuest/0.1 (contact: {contact_email})"
            )
        },
        timeout=(10, 30),
    )
    print("BLS HTTP status:", response.status_code)
    response.raise_for_status()

    parser = DirectoryParser()
    parser.feed(response.text)

    base = urlsplit(BLS_URL)
    files = set()

    for link in parser.links:
        candidate = urlsplit(urljoin(BLS_URL, link))

        # Keep only direct file links inside the intended directory.
        if (
            candidate.scheme != base.scheme
            or candidate.netloc != base.netloc
            or candidate.query
            or candidate.fragment
            or not candidate.path.startswith(base.path)
        ):
            continue

        relative_path = candidate.path[len(base.path):]

        if relative_path and "/" not in relative_path:
            files.add(relative_path)

    if not files:
        raise ValueError("No source files found in the directory response.")

    print("BLS files discovered:", len(files))
    for filename in sorted(files):
        print(" -", filename)

    print("PASS: BLS directory accessible")


def check_population():
    response = requests.get(
        POPULATION_URL,
        headers={"Accept": "application/json"},
        timeout=(10, 30),
    )
    print("Population HTTP status:", response.status_code)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object.")

    records = payload.get("data")
    if not isinstance(records, list) or not records:
        raise ValueError("Expected a nonempty data list.")

    print("Population records:", len(records))
    print("Pagination metadata:", payload.get("page"))
    print("First record:", records[0])
    print("PASS: Population API accessible")


def main():
    contact_email = os.environ.get("BLS_CONTACT_EMAIL", "").strip()
    if not contact_email or "@" not in contact_email:
        raise ValueError("Set BLS_CONTACT_EMAIL before running this script.")

    failures = []

    for name, check in [
        ("BLS", lambda: check_bls(contact_email)),
        ("Population", check_population),
    ]:
        print(f"\n=== {name} check ===")
        try:
            check()
        except (requests.RequestException, ValueError) as error:
            failures.append(name)
            print(f"FAIL: {type(error).__name__}: {error}")

    if failures:
        raise SystemExit(
            "\nSource checks failed: " + ", ".join(failures)
        )

    print("\nPASS: Both source-access checks succeeded")


if __name__ == "__main__":
    main()