from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_ROOT = spark.conf.get("rearc.raw_root", "/Volumes/workspace/rearc/raw").rstrip("/")
MANIFEST_PATH = f"{RAW_ROOT}/manifests"

MANIFEST_SCHEMA = """
    manifest_version INT,
    run_id STRING,
    status STRING,
    started_at_utc STRING,
    completed_at_utc STRING,
    bls_file_count INT,
    population_record_count INT,
    files ARRAY<STRUCT<
        relative_path: STRING,
        source_url: STRING,
        retrieved_at_utc: STRING,
        size_bytes: BIGINT,
        sha256: STRING,
        volume_path: STRING
    >>
"""


@dp.table(
    name="bronze_manifests",
    comment="Published source snapshot manifests, ingested incrementally."
)
@dp.expect_all_or_fail({
    "supported_manifest_version": "manifest_version = 1",
    "published_snapshot": "status = 'published'",
    "run_id_present": "run_id IS NOT NULL AND length(trim(run_id)) > 0",
    "valid_completion_time": "completed_at IS NOT NULL",
    "positive_bls_file_count": "bls_file_count > 0",
    "positive_population_count": "population_record_count > 0",
    "expected_file_count": "size(files) = bls_file_count + 1",
})
def bronze_manifests():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .option("cloudFiles.schemaEvolutionMode", "none")
        .schema(MANIFEST_SCHEMA)
        .load(MANIFEST_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("manifest_file_path"),
        )
        .withColumn(
            "completed_at",
            F.expr("try_cast(completed_at_utc AS TIMESTAMP)")
        )
    )
