# Databricks notebook source
"""Run independent Gold validation and persist execution evidence."""

import contextlib
import io
import runpy
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

dbutils.widgets.text("validation_script", "")
dbutils.widgets.text("workflow_run_id", "manual")
validation_script = dbutils.widgets.get("validation_script")
workflow_run_id = dbutils.widgets.get("workflow_run_id")

if not validation_script:
    raise ValueError("validation_script must point to tests/validate_gold.py")

script_path = Path(validation_script)
if not script_path.is_file():
    raise FileNotFoundError(str(script_path))

# The existing validation script explicitly targets workspace.rearc.
SOURCE_SCHEMA = "workspace.rearc"
RESULT_TABLE = "workspace.rearc_ops.validation_runs"

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.rearc_ops")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {RESULT_TABLE} (
    validation_id STRING,
    workflow_run_id STRING,
    snapshot_run_id STRING,
    source_schema STRING,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status STRING,
    validation_output STRING,
    error_details STRING
)
USING DELTA
""")

def current_snapshot():
    rows = (
        spark.table(f"{SOURCE_SCHEMA}.silver_snapshot_files")
        .select("run_id")
        .distinct()
        .collect()
    )
    if len(rows) != 1 or rows[0]["run_id"] is None:
        raise ValueError("Expected exactly one non-null current snapshot ID")
    return rows[0]["run_id"]

validation_id = str(uuid.uuid4())
started_at = datetime.now(timezone.utc)
snapshot_run_id = None
status = "FAIL"
error_details = None
failure = None
output = io.StringIO()

try:
    snapshot_run_id = current_snapshot()

    with contextlib.redirect_stdout(output):
        runpy.run_path(
            str(script_path),
            init_globals={"spark": spark},
            run_name="__main__",
        )

    if current_snapshot() != snapshot_run_id:
        raise RuntimeError("Current snapshot changed during validation")

    status = "PASS"
except Exception as exc:
    failure = exc
    error_details = traceback.format_exc()
finally:
    finished_at = datetime.now(timezone.utc)

    result = [(
        validation_id,
        workflow_run_id,
        snapshot_run_id,
        SOURCE_SCHEMA,
        started_at,
        finished_at,
        status,
        output.getvalue(),
        error_details,
    )]

    result_schema = """
        validation_id STRING,
        workflow_run_id STRING,
        snapshot_run_id STRING,
        source_schema STRING,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        status STRING,
        validation_output STRING,
        error_details STRING
    """

    (
        spark.createDataFrame(result, schema=result_schema)
        .write.mode("append")
        .saveAsTable(RESULT_TABLE)
    )

print(output.getvalue())
print(f"Validation ID: {validation_id}")
print(f"Snapshot: {snapshot_run_id}")
print(f"Status: {status}")

if failure is not None:
    raise RuntimeError(
        f"Gold validation failed. See {RESULT_TABLE}, "
        f"validation_id={validation_id}"
    ) from failure
