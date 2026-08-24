# HDB Resale Flat Prices Technical Test

This repository contains a Python ETL pipeline and AWS solution architecture for the HDB Senior Data Engineer technical test.

Dataset source: https://data.gov.sg/collections/189/view

The pipeline processes HDB resale flat price records for the assignment period, `2012-01` to `2016-12`, and produces the required raw, cleaned, transformed, failed, and hashed outputs.

## Project Overview

The solution is designed as a reproducible batch data pipeline:

```text
data.gov.sg API / raw CSV files
        |
        v
Extract raw files and combine source files
        |
        v
Filter to 2012-01 through 2016-12 master dataset
        |
        v
Profile + deterministic data quality validation
        |
        +--> failed dataset
        |
        v
Cleaned dataset
        |
        +--> DQC review result for rare values and price anomalies
        |
        v
Transform with Resale Identifier
        |
        v
Hash identifier while preserving uniqueness
        |
        v
Final assignment outputs
```

Main design considerations:

- Preserve raw data as-is for audit and replay.
- Keep deterministic failures separate from review-only DQC findings.
- Make transformations idempotent and testable.
- Keep source file and row number metadata for traceability.
- Use modular Python code rather than one large notebook script.
- Include architecture diagrams for operationalising the flow on AWS.

## Project Structure

```text
hdb_resale_tech_test/
  architecture/              AWS ingestion and exploitation diagrams
  data/
    raw/                     Raw CSV files from data.gov.sg
    cleaned/                 Records that pass deterministic validation
    transformed/             Cleaned records with Resale Identifier
    failed/                  Records removed by hard validation rules
    hashed/                  Cleaned records with hashed identifier
    dqc_result/              Review queue for statistical DQC findings
    profile/                 Data profiling JSON and category domain table
  docs/                      Data quality notes and future improvements
  notebooks/                 Jupyter execution walkthrough
  src/hdb_resale_etl/        ETL package
  tests/                     Unit tests
```

## Requirement-To-Code Map

| Assignment requirement | Implementation | Code location |
| --- | --- | --- |
| Extract data programmatically from data.gov.sg | Discover collection child datasets, filter by coverage period, use initiate/poll download API | `src/hdb_resale_etl/extract.py` |
| Preserve and combine raw source files | Stream downloads to temporary files, verify `Content-Length`, atomically rename, and load CSVs in chunks with source metadata | `download_dataset()` and `load_raw_files()` in `extract.py` |
| Build assignment master dataset | Filter combined raw rows to `2012-01` through `2016-12` before profiling and DQ | `split_assignment_scope()` in `scope.py` |
| Data profiling | Generate strict JSON profile with real missing counts, top values, quantiles, format failures, duplicate-key statistics, and category domain tables | `src/hdb_resale_etl/profile.py` |
| Validate date, town, flat type, flat model, storey range | Apply deterministic validation, storey bounds, and statistical category-domain DQC checks | `src/hdb_resale_etl/quality.py` |
| Recompute remaining lease | Recalculate 99-year lease balance as of run date | `recompute_remaining_lease()` in `quality.py` |
| Handle duplicate composite keys | Use all columns except resale price as the key; keep higher resale price | `split_duplicate_keys()` in `quality.py` |
| Identify anomalous resale prices | 3x IQR rule on `price_per_sqm` within `month + town + flat_type + remaining_lease_decade` groups | `build_price_anomaly_dqc()` in `quality.py` |
| Create Resale Identifier | Apply assignment formula using block, group average price, month, and town | `add_resale_identifier()` in `transform.py` |
| Hash identifier irreversibly | SHA-256 hash using the assignment identifier plus an explicit stable source business key | `add_hashed_identifier()` in `transform.py` |
| Produce output groups | Write raw, cleaned, transformed, failed, hashed, DQC, and profile outputs | `src/hdb_resale_etl/pipeline.py` |
| Provide execution guide | Notebook walkthrough | `notebooks/HDB_Resale_ETL_Walkthrough.ipynb` |
| Provide AWS architecture | PNG diagrams and notes | `architecture/` |

## Data Quality Approach

### Hard Validation Rules

Records that fail these rules are written to `data/failed/failed_resale_flat_prices.csv`. Valid rows outside `2012-01` through `2016-12` are excluded before DQ and are not treated as failed records.

| Rule | Failure reason |
| --- | --- |
| Mandatory fields must not be null or empty | `missing_required_<column>` |
| Business fields must not contain replacement/control characters | `garbled_or_control_characters` |
| `month` must be strict `YYYY-MM` | `invalid_month` |
| `storey_range` must follow `number TO number`, e.g. `01 TO 03` | `invalid_storey_range_format` |
| `storey_range` lower bound must not exceed upper bound and upper bound must be <= 60 | `invalid_storey_range_bounds`, `invalid_storey_range_above_60` |
| `lease_commence_date` must be between 1960 and the run year | `invalid_lease_commence_date` |
| `floor_area_sqm` must be greater than zero | `invalid_floor_area_sqm` |
| `resale_price` must be greater than or equal to zero | `invalid_resale_price` |
| Duplicate composite keys keep only the higher resale price | `duplicate_composite_key_lower_price` |

### DQC Review Rules

Some records pass hard validation but should still be reviewed. These are written to:

```text
data/dqc_result/dqc_result.csv
```

| DQC category | Method | Why it is review-only |
| --- | --- | --- |
| `rare statistical-domain value` | Frequency check on standardized `town`, `flat_type`, `flat_model`, and `storey_range`; values with count <= 1 or frequency <= 0.01% are flagged and listed in `data/profile/category_domain_table.csv` | Rare does not always mean wrong |
| `anomaly resale price` | 3x IQR outlier rule on `price_per_sqm` within `month + town + flat_type + remaining_lease_decade` groups, with `dqc_anomaly_direction` as `high` or `low` | High or low price can still be genuine |

DQC records remain in the cleaned dataset unless a future manual review process rejects them. See `docs/data_quality_notes.md` for the proposed review decision loop.

## Identifier And Hashing

The assignment-defined `resale_identifier` can repeat because it uses only a small number of fields.

To preserve uniqueness, the pipeline hashes:

```text
resale_identifier + cleaned transaction natural key
```

using SHA-256. This produces a 64-character irreversible hash in `hashed_resale_identifier`.

## Architecture Deliverables

```text
architecture/hdb_resale_architecture.png
```


The proposed production architecture uses:

- EventBridge Scheduler to trigger the ECS Fargate ETL task.
- ECS Fargate in a private subnet to extract, validate, transform, and load HDB resale data.
- NAT Gateway for outbound access to data.gov.sg and AWS services without VPC endpoints.
- S3 Gateway Endpoint for private access to the S3 raw and curated zones.
- Glue Data Catalog and Athena to manage metadata and query curated data.
- Athena Interface Endpoint for private connectivity from Tableau.
- CloudWatch, CloudTrail, EventBridge, SNS, and SQS for monitoring, auditing, retry handling, and failure alerts.

Scheduler invocation failures are handled through retry policies and an SQS DLQ. ECS runtime failures are detected through task-state change events and routed to the configured alerting process.

## Future Improvement

Recommended enhancements for a production version:

- Develop a governed dimensional model consisting of `dim_property`, `dim_owner`, and `fact_resale_transaction`, where authoritative internal source data is available. Maintain one row per transaction in the fact table, use stable `property_id` and `owner_id` values as dimension business keys, and use a unique `transaction_id` as the fact table's primary key, with foreign keys linking each transaction to the relevant property, buyer, and seller records.
- Implement the DQC review and master-data maintenance workflow described in `docs/data_quality_notes.md`, including approval and rejection decisions, audit history, and controlled updates to governed property and owner records.
- Publish curated datasets to Amazon S3 as year/month-partitioned Parquet files and extend the current chunked ingestion process to support direct S3 multipart uploads and scalable processing with Glue/Spark or PyArrow without materialising the complete dataset in a single in-memory DataFrame.
- Introduce a CI workflow to run unit tests, schema validation, and critical data-quality assertions automatically for every code change.
