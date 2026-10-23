"""Create isolated test snapshots with an added or removed optional file."""

import argparse
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

TEST_FILE = "bls/pr.test_notes"
TEST_CONTENT = b"Synthetic integration-test documentation; not BLS data.\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["add", "remove"])
    args = parser.parse_args()

    pointer = Path(
        ".local/population_change_snapshot.txt"
        if args.action == "add"
        else ".local/file_add_snapshot.txt"
    )
    baseline = Path(pointer.read_text(encoding="utf-8").strip())
    manifest = json.loads(
        (baseline / "manifest.json").read_text(encoding="utf-8")
    )

    entries = manifest["files"]
    names = {entry["relative_path"] for entry in entries}

    if args.action == "add":
        assert TEST_FILE not in names, "Test file already exists."
    else:
        assert TEST_FILE in names, "Test file is missing from the baseline."

    # Verify the entire input snapshot before creating the next one.
    for entry in entries:
        content = (baseline / entry["relative_path"]).read_bytes()
        assert len(content) == entry["size_bytes"]
        assert hashlib.sha256(content).hexdigest() == entry["sha256"]

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex
    destination = Path(".local/test_snapshots") / run_id
    destination.mkdir(parents=True, exist_ok=False)

    retained = [
        entry for entry in entries
        if not (
            args.action == "remove"
            and entry["relative_path"] == TEST_FILE
        )
    ]

    for entry in retained:
        target = destination / entry["relative_path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(baseline / entry["relative_path"], target)

    if args.action == "add":
        (destination / TEST_FILE).write_bytes(TEST_CONTENT)
        retained.append({
            "relative_path": TEST_FILE,
            "source_url": "synthetic://integration-test/pr.test_notes",
            "retrieved_at_utc": now.isoformat(),
            "size_bytes": len(TEST_CONTENT),
            "sha256": hashlib.sha256(TEST_CONTENT).hexdigest(),
        })

    manifest.update({
        "run_id": run_id,
        "status": "download_validated",
        "started_at_utc": now.isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "bls_file_count": sum(
            entry["relative_path"].startswith("bls/")
            for entry in retained
        ),
        "files": retained,
        "synthetic_test": {
            "scenario": f"optional_file_{args.action}",
            "baseline_run_id": baseline.name,
        },
    })

    (destination / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    Path(f".local/file_{args.action}_snapshot.txt").write_text(
        str(destination), encoding="utf-8"
    )

    print(f"PASS: File-{args.action} snapshot created.")
    print("Snapshot:", destination)
    print("Source files:", len(retained))


if __name__ == "__main__":
    main()