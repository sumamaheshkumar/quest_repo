from pyspark import pipelines as dp
from pyspark.sql import functions as F

POPULATION_SCHEMA = """
    page STRUCT<
        limit: BIGINT,
        offset: BIGINT,
        total: BIGINT
    >,
    data ARRAY<STRUCT<
        `Nation ID`: STRING,
        Nation: STRING,
        Year: STRING,
        Population: STRING
    >>
"""


@dp.table(
    name="bronze_population",
    comment="Population records parsed once per immutable source-file version."
)
@dp.expect_all_or_fail({
    "complete_response": (
        "page_offset = 0 AND page_total = response_record_count"
    ),
    "nonempty_response": "response_record_count > 0",
    "nation_id_present": (
        "nation_id IS NOT NULL AND length(trim(nation_id)) > 0"
    ),
    "expected_nation": (
        "nation_id = '01000US' AND nation = 'United States'"
    ),
    "year_present": (
        "year_raw IS NOT NULL AND length(trim(year_raw)) > 0"
    ),
    "population_present": (
        "population_raw IS NOT NULL "
        "AND length(trim(population_raw)) > 0"
    ),
})
def bronze_population():
    documents = (
        spark.readStream.table("bronze_source_files")
        .filter(
            F.col("relative_path") == "population/population.json"
        )
        .select(
            "volume_path",
            "sha256",
            F.from_json(
                F.decode("content", "UTF-8"),
                POPULATION_SCHEMA,
                {"mode": "FAILFAST"},
            ).alias("document"),
        )
        .select(
            "volume_path",
            "sha256",
            F.col("document.page.offset").alias("page_offset"),
            F.col("document.page.total").alias("page_total"),
            F.size("document.data").alias("response_record_count"),
            F.col("document.data").alias("records"),
        )
    )

    # Outer explode retains an invalid empty document as a row,
    # allowing expectations to fail instead of silently losing it.
    records = documents.select(
        "volume_path",
        "sha256",
        "page_offset",
        "page_total",
        "response_record_count",
        F.posexplode_outer("records").alias("record_index", "record"),
    )

    return records.select(
        "volume_path",
        "sha256",
        "page_offset",
        "page_total",
        "response_record_count",
        "record_index",
        F.col("record.`Nation ID`").alias("nation_id"),
        F.col("record.Nation").alias("nation"),
        F.col("record.Year").alias("year_raw"),
        F.col("record.Population").alias("population_raw"),
    )