# Rearc Data Quest

A snapshot-based BLS and US population ingestion workflow with Databricks
Bronze, Silver, and Gold tables.

## Implementation

- Source retrieval: Python in GitHub Codespaces.
- Raw storage: Databricks Unity Catalog Volume.
- Processing: Lakeflow Spark Declarative Pipeline.
- Published analytical answers: SQL materialized views.
- Alternative implementations and comparisons: PySpark.

Codespaces handles source retrieval because the tested Databricks environment
could not resolve the source hostnames. All Spark transformations run in
Databricks.

See [PROCESS.md](PROCESS.md) for architecture, decisions, and limitations.
See [validation results](docs/validation_results.md) for tested outcomes.

## Repository structure

- `config/`: main and isolated-test ingestion configuration.
- `src/ingestion/`: HTTP client, discovery, download, and publication.
- `src/pipelines/`: Python Bronze/Silver and SQL Gold definitions.
- `tests/validate_gold.py`: all three PySpark alternatives and comparisons.
- `tests/test_source_validation.py`: population-validator unit tests.
- Other `tests/` scripts: controlled integration-test fixtures.
- `docs/`: validation evidence and monitoring/performance queries.

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless pipelines.
- Permission to create a schema, managed Volume, and pipeline.
- A Python environment with outbound access to both sources.
- Databricks CLI installed and authenticated.
- GitHub Codespaces was used for the external Python environment.

The tested Codespaces environment used Python 3.14.
Python dependencies are pinned in `requirements.txt`.
Databricks supplies the Spark runtime; do not install local PySpark to run
the pipeline.

## 1. Prepare the external Python environment

Run from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m pip check
```

Set your contact email for BLS requests:

```bash
export BLS_CONTACT_EMAIL="your-email@example.com"
```

This variable must be available in each ingestion terminal or execution
environment.

Authenticate the CLI to your workspace:

```bash
databricks auth login --host https://YOUR-WORKSPACE-HOST --profile rearc
databricks auth profiles
```

Replace the host placeholder. Credentials must remain outside the repository.

In browser-based Codespaces, a localhost OAuth callback may need to be
delivered to the waiting CLI inside Codespaces rather than the local PC.
Never share authorization callback URLs or tokens.

## 2. Create Databricks storage

Run in a Databricks SQL notebook:

```sql
CREATE SCHEMA IF NOT EXISTS workspace.rearc;
CREATE VOLUME IF NOT EXISTS workspace.rearc.raw;
```

Review `config/ingestion.json` before running ingestion. Adjust the profile
and Volume path for your environment.

## 3. Download and publish a snapshot

```bash
PYTHONPATH=src python -m ingestion.download
```

The command prints the completed snapshot directory.

Publish that exact directory, replacing `ACTUAL_RUN_ID`:

```bash
PYTHONPATH=src python -m ingestion.publish ".local/snapshots/ACTUAL_RUN_ID"
```

Do not publish a failed download directory without a completed manifest.

Publication verifies remote content and writes the manifest last.
Unchanged content is reused. Run publishers sequentially.

The current downloader reads source content again to compute hashes;
unchanged remote uploads and repeated Bronze parsing are avoided.
HTTP conditional downloading is not implemented.

## 4. Deploy the Databricks pipeline with Asset Bundles

Deployment configuration is in `databricks.yml`. Source ingestion remains
a separate Codespaces step; the bundle manages the transformation pipeline.

For another workspace, update the workspace host, production root path,
catalog, schema, raw_root, and notification_email before deployment.
Prepare storage and publish a snapshot using the preceding steps.

### Adopt an existing pipeline

For this project, the existing main pipeline was bound before deployment:

```bash
databricks bundle deployment bind rearc_pipeline 45bd5538-e874-4583-aae4-7775c51ba9fb -t prod -p rearc
```

Binding is a one-time adoption step. The ID above belongs to the author's
workspace. Use your own existing pipeline ID elsewhere, or skip binding
when creating a new pipeline. Do not create a second pipeline targeting
tables owned by an existing pipeline.

### Validate, plan, deploy, and run

Run from the repository root:

```bash
databricks bundle validate -t prod -p rearc
databricks bundle plan -t prod -p rearc
databricks bundle deploy -t prod -p rearc
databricks bundle run rearc_pipeline -t prod -p rearc
```

Review the plan before deploying. The verified deployment updated one
existing pipeline, with no resources created or deleted.

Pipeline sources come from `src/pipelines/`. The pipeline is serverless,
triggered, and uses `rearc.raw_root` for its input location.
Routine updates do not require a full refresh.
Local `.databricks/` state is excluded from Git.

## 5. Inspect the assessment outputs

```sql
SELECT *
FROM workspace.rearc.gold_population_statistics;

SELECT *
FROM workspace.rearc.gold_best_year
ORDER BY series_id;

SELECT *
FROM workspace.rearc.gold_target_series_population
ORDER BY year;
```

Question 1 exposes both sample and population standard deviation.
Question 2 excludes annual averages, reports partial years, and selects
the earliest year when maximum sums tie.
Question 3 retains observations without a population match.

## 6. Validate

Copy `tests/validate_gold.py` into a Databricks Python notebook and run it.
It requires the Databricks-provided `spark` session.

It compares:
- SQL and PySpark population statistics with Python statistics.
- Every SQL best-year result with a PySpark alternative.
- Every SQL target-series join result with a PySpark alternative.
- Gold snapshot IDs with the current file inventory.

Source-validation unit tests run in Codespaces:

```bash
PYTHONPATH=src python -m unittest discover -s tests -p 'test_source_validation.py' -v
```

The current unit tests load a local baseline snapshot. On a fresh checkout,
update their `BASELINE` path after downloading a snapshot.

## 7. Isolated integration tests

Test destination:
- Schema: `workspace.rearc_test`
- Volume: `/Volumes/workspace/rearc_test/raw`
- Pipeline: `rearc_data_quest_test`
- Configuration file: `config/ingestion_test.json`

The test pipeline uses the same transformation code and overrides
`rearc.raw_root` to the test Volume.

Integration fixtures are synthetic and must never be published to the main
Volume. The fixture scripts depend on baseline paths and local pointer files
created during the documented test sequence; they are not a standalone CI
suite.

See `docs/validation_results.md` for tested scenarios.

## Operations and limitations

- `docs/performance_checks.sql` inspects grouping sizes and a join plan.
- Codespaces ingestion and pipeline execution are currently separate,
  manually initiated steps.
- No unattended scheduler or concurrent-publisher coordination is implemented.
- Pipeline notification delivery requires separate verification.
- Source data and credentials are excluded from Git.
- Synthetic fixtures and downloaded snapshots are excluded through `.local/`.

See `PROCESS.md` for additional operational limitations and AI assistance.

## Bonus deliverables

### Gold-layer dashboard

The published dashboard includes population statistics for 2013–2018,
a BLS Q01 trend, best-year results, and BLS/population comparisons.

- [Dashboard definition](dashboards/rearc_gold_analytics.lvdash.json)
- [Dashboard screenshot](dashboards/rearc_gold_dashboard.png)

The dashboard was created in the UI and exported to Git. It is not
currently deployed by the bundle. Live viewing requires appropriate
Databricks access; reviewers can inspect the screenshot and export directly.

Dashboard queries currently assume one ingestion run. Multiple retained
runs would require consistent run filtering. Explanatory counts are static.

### Unity Catalog analyst access

The rearc_analysts group was granted USE CATALOG on workspace,
USE SCHEMA on workspace.rearc, and SELECT on the three Gold tables.

- [Permission script](sql/security/grant_rearc_analysts.sql)

The grants were applied and inspected. No Bronze/Silver reads, raw-file
access, or write privileges are granted by this script. Inherited access
and other group memberships must also be reviewed.

Non-admin access tests have not been performed, so Gold-only effective
access is not yet verified. Analyst dashboard viewing and warehouse usage
permissions also require configuration.

### Verified bundle deployment

The existing main pipeline was deployed and run through the bundle
on 2026-10-11.

- Pipeline ID: 45bd5538-e874-4583-aae4-7775c51ba9fb
- Update ID: ae825d2d-7841-4f94-aa0c-361aa5ee7b3e
- Deployment: 0 resources created, 1 updated, 0 deleted.
- Execution: COMPLETED, with all 12 flows completed.
- Implementation commit: 332e0e8.

This verifies deployment and execution. Data-correctness validation is
documented separately in docs/validation_results.md.

### Data Trust: automated validation evidence

The bundle-managed rearc_data_trust job updates the pipeline, then runs
the independent Gold checks in tests/validate_gold.py. Validation results
are persisted in workspace.rearc_ops.validation_runs.

Run on demand:

    databricks bundle run rearc_data_trust -t prod -p rearc

Implementation:
- resources/data_trust.job.yml
- src/operations/validate_and_record.py

The Data Trust dashboard page shows validation evidence for the current
snapshot, calculation-check output, snapshot completion time, source-year
coverage, and population-match coverage.

Verified workflow run: 395679996482979.
Validation ID: 57389a7e-7cdf-4baf-bc53-b0f03ceb5a0c.
Result: PASS. Validation completed on 2026-10-11 at 23:32:12 UTC.

No schedule is configured. Pipeline failures skip downstream validation.
The dashboard shows recorded validation evidence, not live workflow health.
Validation does not establish upstream source completeness.
The operations notebook currently targets workspace.rearc explicitly.

Data Trust reads Silver, manifest, and operations tables. Existing
Gold-only analyst grants do not authorize this page under individual
data permissions; it currently requires an appropriately authorized operator.

[Data Trust screenshot](dashboards/rearc_data_trust.png)

## Databricks screenshots

Required implementation evidence:
- [Completed pipeline and transformation graph](docs/screenshots/pipeline_completed.png)
- [Unity Catalog tables](docs/screenshots/catalog_tables.png)

Bonus evidence:
- [Successful pipeline and validation workflow](docs/screenshots/data_trust_workflow_success.png)
- [Gold analyst grants](docs/screenshots/gold_analyst_grants.png)
- [Analytics dashboard](dashboards/rearc_gold_dashboard.png)
- [Data Trust dashboard](dashboards/rearc_data_trust.png)

The grant screenshot documents configured permissions, not a completed
non-admin access test.
