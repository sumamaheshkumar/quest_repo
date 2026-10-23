"""Publish a validated local snapshot to a Databricks Volume."""

import argparse
import hashlib
import io
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound


def sha256(content):
    return hashlib.sha256(content).hexdigest()


def read_remote(client, path):
    """Return bytes, or None only when the file does not exist."""
    try:
        response = client.files.download(path)
    except NotFound:
        return None

    if response.contents is None:
        raise RuntimeError(f"Missing download response body: {path}")

    with response.contents as stream:
        return stream.read()


def ensure_remote_file(client, remote_path, content):
    """Create missing content; verify existing content without overwriting."""
    existing = read_remote(client, remote_path)

    if existing is not None:
        if existing != content:
            raise RuntimeError(
                f"Existing remote content does not match: {remote_path}"
            )
        return "reused"

    parent = remote_path.rsplit("/", 1)[0]
    client.files.create_directory(parent)

    client.files.upload(
        remote_path,
        io.BytesIO(content),
        overwrite=False,
    )

    uploaded = read_remote(client, remote_path)
    if uploaded != content:
        raise RuntimeError(f"Upload verification failed: {remote_path}")

    return "uploaded"


def publish_snapshot(snapshot_dir, config):
    local_manifest = snapshot_dir / "manifest.json"
    manifest = json.loads(local_manifest.read_text(encoding="utf-8"))

    if manifest.get("status") != "download_validated":
        raise ValueError("Snapshot has not passed download validation.")

    if manifest["run_id"] != snapshot_dir.name:
        raise ValueError("Snapshot directory and manifest run ID differ.")

    entries = manifest["files"]
    paths = [entry["relative_path"] for entry in entries]

    if len(paths) != len(set(paths)):
        raise ValueError("Duplicate paths in manifest.")

    if len(entries) != manifest["bls_file_count"] + 1:
        raise ValueError("Unexpected source file count.")

    # Validate every local file before making any remote changes.
    snapshot_root = snapshot_dir.resolve()
    validated = []

    for entry in entries:
        local_path = (snapshot_dir / entry["relative_path"]).resolve()

        if not local_path.is_relative_to(snapshot_root):
            raise ValueError("Manifest path escapes the snapshot directory.")

        content = local_path.read_bytes()

        if (
            len(content) != entry["size_bytes"]
            or sha256(content) != entry["sha256"]
        ):
            raise ValueError(
                f"Local integrity check failed: {entry['relative_path']}"
            )

        validated.append((entry, content))

    root = config["volume_root"].removeprefix("dbfs:").rstrip("/")
    if not root.startswith("/Volumes/"):
        raise ValueError("Expected a Unity Catalog Volume path.")

    client = WorkspaceClient(profile=config["databricks_profile"])

    uploaded = 0
    reused = 0
    published_entries = []

    for entry, content in validated:
        remote_path = (
            f"{root}/objects/{entry['sha256']}/"
            f"{entry['relative_path']}"
        )

        outcome = ensure_remote_file(client, remote_path, content)

        if outcome == "uploaded":
            uploaded += 1
        else:
            reused += 1

        published_entries.append({
            **entry,
            "volume_path": remote_path,
        })
        print(f"{outcome.upper()}: {entry['relative_path']}")

    # Deterministic bytes make publishing the same snapshot repeatable.
    published_manifest = {
        **manifest,
        "status": "published",
        "files": published_entries,
    }
    manifest_bytes = (
        json.dumps(published_manifest, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")

    remote_manifest = (
        f"{root}/manifests/{manifest['run_id']}.json"
    )

    # Publish last: downstream processing must use manifests, not scan objects.
    manifest_outcome = ensure_remote_file(
        client, remote_manifest, manifest_bytes
    )

    print(f"\nSource files uploaded: {uploaded}")
    print(f"Source files reused: {reused}")
    print(f"Manifest: {manifest_outcome}")
    print(f"Manifest path: {remote_manifest}")
    print("PASS: Remote source bytes match the validated local snapshot.")
    print("PASS: Published manifest verified.")


# def main():
#     parser = argparse.ArgumentParser()
#     parser.add_argument("snapshot_dir", type=Path)
#     args = parser.parse_args()

#     config = json.loads(
#         Path("config/ingestion.json").read_text(encoding="utf-8")
#     )
#     publish_snapshot(args.snapshot_dir, config)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_dir", type=Path)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/ingestion.json"),
    )
    args = parser.parse_args()

    config = json.loads(
        args.config.read_text(encoding="utf-8")
    )

    print("Publication destination:", config["volume_root"])
    publish_snapshot(args.snapshot_dir, config)


if __name__ == "__main__":
    main()