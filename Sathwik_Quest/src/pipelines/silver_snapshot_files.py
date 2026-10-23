from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

REQUIRED_FILES = [
    "bls/pr.data.1.AllData",
    "bls/pr.series",
    "bls/pr.sector",
    "bls/pr.class",
    "bls/pr.measure",
    "bls/pr.duration",
    "bls/pr.seasonal",
    "bls/pr.period",
    "population/population.json",
]


@dp.materialized_view(
    name="silver_snapshot_files",
    comment="Validated file inventory for the latest published snapshot."
)
@dp.expect_all_or_fail({
    "valid_snapshot_start": "snapshot_started_at IS NOT NULL",
    "unique_manifest_paths": (
        "manifest_file_count = distinct_relative_paths"
    ),
    "required_files_present": "missing_required_count = 0",
    "source_file_available": "observed_sha256 IS NOT NULL",
    "matching_hash": "sha256 = observed_sha256",
    "matching_size": "size_bytes = observed_size_bytes",
    "complete_inventory": "joined_file_count = manifest_file_count",
})
def silver_snapshot_files():
    manifests = (
        spark.read.table("bronze_manifests")
        .withColumn(
            "snapshot_started_at",
            F.expr("try_cast(started_at_utc AS TIMESTAMP)"),
        )
    )

    latest = (
        manifests
        .withColumn(
            "_snapshot_rank",
            F.row_number().over(
                Window.orderBy(
                    F.col("snapshot_started_at").desc(),
                    F.col("run_id").desc(),
                )
            ),
        )
        .filter(F.col("_snapshot_rank") == 1)
        .withColumn("manifest_file_count", F.size("files"))
        .withColumn(
            "_relative_paths",
            F.transform("files", lambda entry: entry["relative_path"]),
        )
        .withColumn(
            "distinct_relative_paths",
            F.size(F.array_distinct("_relative_paths")),
        )
        .withColumn(
            "missing_required_count",
            F.size(
                F.array_except(
                    F.array(*[F.lit(path) for path in REQUIRED_FILES]),
                    F.col("_relative_paths"),
                )
            ),
        )
    )

    expected = (
        latest.select(
            "run_id",
            "snapshot_started_at",
            "manifest_file_count",
            "distinct_relative_paths",
            "missing_required_count",
            F.explode("files").alias("file"),
        )
        .select(
            "run_id",
            "snapshot_started_at",
            "manifest_file_count",
            "distinct_relative_paths",
            "missing_required_count",
            "file.*",
        )
    )

    observed = spark.read.table("bronze_source_files").select(
        "volume_path",
        F.col("sha256").alias("observed_sha256"),
        F.col("size_bytes").alias("observed_size_bytes"),
    )

    return (
        expected.join(observed, "volume_path", "left")
        .withColumn(
            "joined_file_count",
            F.count(F.lit(1)).over(Window.partitionBy("run_id")),
        )
    )