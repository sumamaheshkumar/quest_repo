# Implementation process and decisions

## Objective

Ingest the BLS productivity directory and the DataUSA population response
into a Databricks Volume, build Bronze/Silver/Gold pipeline tables, and
answer the three analytical questions in SQL and PySpark.

## Architecture

This is a triggered batch workflow.

1. A Python ingestion process in GitHub Codespaces retrieves source data.
2. It dynamically discovers direct files in the BLS directory.
3. It validates downloads and records file sizes and SHA-256 hashes.
4. It publishes immutable file versions to a Unity Catalog Volume.
5. It publishes a snapshot manifest after verifying all referenced uploads.
6. A Lakeflow Spark Declarative Pipeline incrementally ingests file versions
   and manifests into Bronze.
7. Silver selects the latest published snapshot, validates its inventory,
   converts values, and joins readable metadata.
8. Gold produces the three assessment outputs.

Streaming tables provide incremental file tracking inside a triggered
pipeline. The workflow is not a continuously running event stream.

## Environment decision

The Databricks environment could not resolve download.bls.gov or
honolulu-api.datausa.io, while the sources were accessible from the browser
and GitHub Codespaces.

Source retrieval therefore runs in Codespaces. Storage and all Spark
transformations run in Databricks. No local Python installation is required.

BLS requests include a User-Agent with contact information supplied through
BLS_CONTACT_EMAIL. DNS failures and HTTP 403 responses are distinct issues;
the contact header does not fix DNS resolution.

The Codespaces-to-Databricks connection uses the Databricks CLI OAuth
profile named rearc. Credentials are not stored in this repository.

## Storage and snapshot semantics

Raw source bytes are preserved under:

    /Volumes/workspace/rearc/raw/objects/<sha256>/<relative_path>

Completed manifests are stored under:

    /Volumes/workspace/rearc/raw/manifests/<run_id>.json

Each manifest records the source inventory, source URLs, retrieval times,
file sizes, hashes, and Volume paths.

Unchanged files reuse their existing content-addressed paths. Changed files
create new versions. Removed files disappear from the current manifest
while historical bytes remain available.

The current snapshot is selected by download start time, with run ID as a
deterministic tie-breaker. Runs are operated sequentially.

Manifest-last publication keeps uncommitted objects out of current
analytical selection. Bronze may retain and parse unpublished objects.

There is no claim of a distributed transaction across source downloads,
Volume files, or all pipeline tables.

## Bronze, Silver, and Gold

### Bronze

- bronze_manifests: published snapshot documents.
- bronze_source_files: original file bytes and integrity metadata.
- bronze_bls_observations: versioned AllData observation rows.
- bronze_population: versioned population records.
- bronze_bls_metadata: versioned metadata rows.

Auto Loader checkpoints track ingested paths. Normal reruns and unchanged
snapshots were verified not to duplicate parsed observations.

### Silver

- silver_snapshot_files: validated current file inventory.
- silver_bls_observations: typed observations with unique business keys.
- silver_population: typed US population with unique nation/year keys.
- silver_series: readable series definitions.

Metadata codes remain strings to preserve leading zeros. Missing labels,
invalid numeric values, and duplicate keys are treated as failures.

AllData is the analytical observation source. Current is retained in raw
storage but is not appended to AllData, avoiding overlapping observations.

### Gold

- gold_population_statistics
- gold_best_year
- gold_target_series_population

SQL defines the published Gold tables. tests/validate_gold.py contains the
PySpark alternatives and compares their results with SQL.

## Analytical interpretation

### Population statistics

Use US population for 2013–2018 inclusive. Require six distinct years.
Expose both sample and population standard deviations with explicit names.

### Best year per series

Sum Q01–Q04 observations and exclude Q05, which BLS defines as Annual Average.

Retain partial years and expose quarter_count. Choose the earliest year
when sums tie and expose tied_year_count. Preserve series without quarterly
data with a NO_QUARTERLY_DATA status.

The requested sum is not described as compounded annual growth; series
include percentage changes and indexes.

### Target series and population

Filter PRS30006032 to Q01 and left join US population by snapshot and year.
Preserve every target observation. Missing population remains null,
including the absent 2020 population record.

## Reliability and validation

See docs/validation_results.md for observed results and test scope.

Verified scenarios include:
- repeated publication and unchanged-source snapshots;
- changed population content and downstream recalculation;
- optional-file addition and removal;
- incomplete publication isolation and subsequent recovery;
- invalid population input rejection;
- pipeline expectation failure and successful recovery.

Tests used workspace.rearc_test and a separate test Volume. Main analytical
results remained unchanged.

Source requests use connection/read timeouts and bounded retry counts for
configured transient failures. HTTP 403 is not automatically retried.
Read timeouts are inactivity limits, not total workflow deadlines.
Retry exhaustion and end-to-end network failure recovery were not tested.

## Compute and performance

Serverless, triggered pipeline execution fits the small assessment dataset
and available environment. No continuous schedule is enabled.

Observed AllData size: 77,126 rows across 282 series.
Rows per series: minimum 35, maximum 397, mean 273.5, median 392.

The inspected equivalent population-join query selected an initial
PhotonBroadcastHashJoin LeftOuter plan without a broadcast hint. Filters
were applied at scans, and only required columns were projected.

The plan was not a final executed plan and reported missing optimizer
statistics for internal materialization tables. No internal backing tables
were manually modified.

No evidence justified salting keys, forced repartitioning, forced broadcast,
or additional partitioning at this scale. Whole-file binary ingestion is
appropriate for these small files; large files would require reassessment
of memory use and parsing strategy.

One observed main update spent approximately 56 seconds in RUNNING and
172 seconds from WAITING_FOR_RESOURCES through completion. This is a single
lifecycle observation, not a performance benchmark.

## Monitoring and operational boundaries

Pipeline execution was inspected through Databricks update logs. The Data Trust workflow also persists analytical validation evidence in workspace.rearc_ops.validation_runs.
The latest inspected main-pipeline update completed successfully.

Notification configuration and email delivery must be verified separately.
Pipeline notifications do not cover failures in the external Codespaces
download/publish process.

Production follow-up would include:
- durable scheduling and orchestration across ingestion and transformation;
- service authentication appropriate for unattended execution;
- prevention of concurrent publications;
- external-ingestion failure alerts and freshness monitoring;
- retention and cleanup policies;
- portable, self-contained integration-test fixtures;
- cross-table publication controls if readers require an atomic release.

These are deployment limitations, not capabilities claimed as implemented.

## Reproducibility limitations

Integration fixture scripts currently reference locally generated baseline
snapshots. Those snapshots are excluded from Git. Replaying the fixture
sequence requires recreating a baseline and updating the baseline references.

The main ingestion commands support fresh source retrieval. The analytical
validation uses current Silver data rather than permanently hardcoded totals.

## AI assistance and retrospective

AI assistance was used for implementation guidance, code drafting,
troubleshooting, test design, and documentation. The implementation was
executed and its outputs checked in Codespaces and Databricks.

The main lessons were to distinguish DNS failures from HTTP access-policy
errors, separate immutable file history from current snapshot selection,
and validate changes and failure recovery in an isolated environment.

The next improvement would be durable orchestration of external ingestion with the deployed pipeline-and-validation workflow.
with reproducible deployment and automated orchestration.


## Why SQL and PySpark

SQL is primary for Gold because the requested answers are relational
aggregations, rankings, and joins that are concise to review in SQL.
PySpark handles file parsing and metadata transformations in Bronze/Silver.
Separate PySpark implementations validate the SQL outputs; Python statistics
provides an additional reference for the population summary.

These comparisons validate calculations over ingested data. They do not
independently establish upstream source completeness.

## Additional production trade-offs

- Schema drift: explicit schemas and expectations favor visible failures over
  silently accepting incompatible changes. A client implementation would add
  schema-version handling, quarantine, and a reviewed migration process.
- Cost: triggered serverless execution avoids continuous processing for this
  workload. Validation adds compute work. Larger deployments would measure
  billing and runtime, set budgets, and choose validation frequency accordingly.
  Free Edition execution is not a production cost benchmark.
- Access control: rearc_analysts has catalog/schema usage and SELECT on the
  three Gold tables. Direct grants were inspected; non-admin effective-access
  testing remains unverified. Inherited grants must also be reviewed.
- Operational visibility: Data Trust reads Silver, manifests, and operational
  evidence. It is currently an operator page, not covered by Gold-only analyst
  grants. A client implementation could expose curated operational views
  without broad access to underlying tables.

## Bonus implementation and evidence

Databricks Asset Bundles now deploy the existing main pipeline and the
rearc_data_trust job. The job sequences a pipeline update followed by the
independent Gold validation notebook, with one concurrent job run allowed.
External source download and publication remain manually initiated.

The verified workflow run 395679996482979 completed successfully.
Validation 57389a7e-7cdf-4baf-bc53-b0f03ceb5a0c recorded PASS for the
current snapshot, including all three analytical comparisons and
Gold snapshot consistency.

The exported dashboard contains Overview and Data Trust pages.
Data Trust presents recorded validation evidence and source coverage;
it is not live workflow monitoring. Pipeline failures skip validation,
and an earlier PASS does not prove that a later workflow attempt succeeded.

## Hardest part to get right

The main reliability challenge was separating immutable file history from
the current published snapshot. New or partially uploaded objects must not
become current analytical inputs before their complete manifest is published.
The isolated tests exercised interrupted publication, recovery, changed
content, and file-inventory changes to check that distinction.
