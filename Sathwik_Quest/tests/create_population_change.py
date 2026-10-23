"""Create a synthetic population-change snapshot for integration testing."""

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

BASELINE = Path(
    ".local/snapshots/"
    "20261010T224411Z_70a1d510014d4a1bab5107f7131d1c53"
)

manifest = json.loads(
    (BASELINE / "manifest.json").read_text(encoding="utf-8")
)

# Verify the baseline before copying it.
for entry in manifest["files"]:
    content = (BASELINE / entry["relative_path"]).read_bytes()
    assert len(content) == entry["size_bytes"]
    assert hashlib.sha256(content).hexdigest() == entry["sha256"]

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex
destination = Path(".local/test_snapshots") / run_id
destination.mkdir(parents=True, exist_ok=False)

for entry in manifest["files"]:
    target = destination / entry["relative_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(BASELINE / entry["relative_path"], target)

population_path = destination / "population/population.json"
payload = json.loads(population_path.read_text(encoding="utf-8"))

matches = [
    row for row in payload["data"]
    if row["Nation ID"] == "01000US" and row["Year"] == 2013
]
assert len(matches) == 1
assert matches[0]["Population"] == 316128839

matches[0]["Population"] = 316129439

content = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
population_path.write_bytes(content)

for entry in manifest["files"]:
    if entry["relative_path"] == "population/population.json":
        entry["size_bytes"] = len(content)
        entry["sha256"] = hashlib.sha256(content).hexdigest()
        entry["retrieved_at_utc"] = now.isoformat()

manifest.update({
    "run_id": run_id,
    "status": "download_validated",
    "started_at_utc": now.isoformat(),
    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    "synthetic_test": {
        "scenario": "population_2013_plus_600",
        "baseline_run_id": BASELINE.name,
        "note": "Modified fixture; not a fresh source download."
    },
})

(destination / "manifest.json").write_text(
    json.dumps(manifest, indent=2) + "\n",
    encoding="utf-8",
)

# Record the exact fixture directory for the next command.
Path(".local/population_change_snapshot.txt").write_text(
    str(destination), encoding="utf-8"
)

print("PASS: Synthetic test snapshot created.")
print("2013 population: 316129439")
print("Expected six-year mean: 322069908")
print("Snapshot:", destination)