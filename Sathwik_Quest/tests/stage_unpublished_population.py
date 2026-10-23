"""Stage a synthetic file without publishing a snapshot manifest."""

import hashlib
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from ingestion.publish import ensure_remote_file

config = json.loads(
    Path("config/ingestion_test.json").read_text(encoding="utf-8")
)
assert config["volume_root"] == "dbfs:/Volumes/workspace/rearc_test/raw"

baseline = Path(
    Path(".local/file_remove_snapshot.txt")
    .read_text(encoding="utf-8")
    .strip()
)
payload = json.loads(
    (baseline / "population/population.json").read_text(encoding="utf-8")
)

matches = [
    row for row in payload["data"]
    if row["Nation ID"] == "01000US" and row["Year"] == 2013
]
assert len(matches) == 1
assert matches[0]["Population"] == 316129439

# A second distinct value, deliberately left unpublished.
matches[0]["Population"] = 316130039

content = (json.dumps(payload, indent=2) + "\n").encode("utf-8")
digest = hashlib.sha256(content).hexdigest()

root = config["volume_root"].removeprefix("dbfs:")
remote_path = f"{root}/objects/{digest}/population/population.json"

client = WorkspaceClient(profile=config["databricks_profile"])
outcome = ensure_remote_file(client, remote_path, content)

Path(".local/unpublished_population_sha256.txt").write_text(
    digest, encoding="utf-8"
)

print("File:", outcome)
print("SHA-256:", digest)
print("PASS: Test object verified; no manifest was published.")