from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_ROOT = spark.conf.get("rearc.raw_root", "/Volumes/workspace/rearc/raw").rstrip("/")
OBJECTS_PATH = f"{RAW_ROOT}/objects"

BINARY_SCHEMA = """
    path STRING,
    modificationTime TIMESTAMP,
    length BIGINT,
    content BINARY
"""


@dp.table(
    name="bronze_source_files",
    comment="Immutable source-file versions with original bytes and SHA-256."
)
@dp.expect_all_or_fail({
    "content_present": "content IS NOT NULL",
    "nonempty_file": "size_bytes > 0",
    "size_matches_content": "size_bytes = length(content)",
    "valid_hash_in_path": "length(expected_sha256) = 64",
    "hash_matches_content": "sha256 = expected_sha256",
    "relative_path_present": "length(relative_path) > 0",
})
def bronze_source_files():
    raw = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .schema(BINARY_SCHEMA)
        .load(OBJECTS_PATH)
    )

    return (
        raw.select(
            # Normalize an optional dbfs: prefix for later manifest joins.
            F.regexp_replace("path", r"^dbfs:", "").alias("volume_path"),
            F.col("modificationTime").alias("file_modified_at"),
            F.col("length").alias("size_bytes"),
            "content",
        )
        .withColumn(
            "expected_sha256",
            F.regexp_extract(
                "volume_path",
                r"/objects/([0-9a-f]{64})/",
                1,
            ),
        )
        .withColumn(
            "relative_path",
            F.regexp_extract(
                "volume_path",
                r"/objects/[0-9a-f]{64}/(.+)$",
                1,
            ),
        )
        .withColumn("sha256", F.sha2("content", 256))
    )
