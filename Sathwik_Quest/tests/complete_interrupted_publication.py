"""Complete the previously simulated interrupted test publication."""

import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from ingestion.publish import publish_snapshot

config = json.loads(
    Path("config/ingestion_test.json").read_text(encoding="utf-8")
)
assert config["volume_root"] == "dbfs:/Volumes/workspace/rearc_test/raw"

baseline = Path(
    Path(".local/file_remove_snapshot.txt")
    .read_text(encoding="utf-8")
    .strip()
)
manifest = json.loads(
    (baseline / "manifest.json").read_text(encoding="utf-8")
)

now = datetime.now(timezone.utc)
run_id = now.strftime("%Y%m%dT%H%M%SZ") + "_" + uuid4().hex
destination = Path(".local/test_snapshots") / run_id
destination.mkdir(parents=True, exist_ok=False)

for entry in manifest["files"]:
    content = (baseline / entry["relative_path"]).read_bytes()
    assert len(content) == entry["size_bytes"]
    assert hashlib.sha256(content).hexdigest() == entry["sha256"]

    target = destination / entry["relative_path"]
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(baseline / entry["relative_path"], target)

population_path = destination / "population/population.json"
payload = json.loads(population_path.read_text(encoding="utf-8"))

matches = [
    row for row in payload["data"]
    if row["Nation ID"] == "01000US" and row["Year"] == 2013
]
assert len(matches) == 1
assert matches[0]["Population"] == 316129439
matches[0]["Population"] = 316130039

content = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
digest = hashlib.sha256(content).hexdigest()

staged_digest = Path(
    ".local/unpublished_population_sha256.txt"
).read_text(encoding="utf-8").strip()

assert digest == staged_digest, (
    "Recovery content differs from the previously staged file."
)
population_path.write_bytes(content)

for entry in manifest["files"]:
    if entry["relative_path"] == "population/population.json":
        entry.update({
            "size_bytes": len(content),
            "sha256": digest,
            "retrieved_at_utc": now.isoformat(),
        })

manifest.update({
    "run_id": run_id,
    "status": "download_validated",
    "started_at_utc": now.isoformat(),
    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    "synthetic_test": {
        "scenario": "complete_interrupted_publication",
        "baseline_run_id": baseline.name,
    },
})

(destination / "manifest.json").write_text(
    json.dumps(manifest, indent=2) + "\n",
    encoding="utf-8",
)

publish_snapshot(destination, config)