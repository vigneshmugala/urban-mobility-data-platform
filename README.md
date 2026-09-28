# Urban Mobility Data Platform

Production-style batch data platform for NYC Yellow Taxi Trip Data, built with **Python, PySpark, Databricks, Delta Lake, SQL, dbt, and Git**.

The platform ingests monthly NYC TLC Yellow Taxi Parquet files and automatically processes new data through a **Bronze → Silver → Gold** architecture before refreshing an analytics dashboard.

The pipeline is designed around practical data-engineering concerns including **incremental processing, file-level idempotency, record-level deduplication, MERGE-based upserts, data-quality validation, current-state dimensions, SCD Type 2 history, automated dbt testing, and workflow orchestration**.

---

## Architecture

```text
NYC TLC Monthly Parquet Files
            │
            ▼
┌───────────────────────────────┐
│      Databricks Job           │
│                               │
│  1. Bronze Ingestion          │
│  2. dbt Build                 │
│  3. Dashboard Refresh         │
└───────────────┬───────────────┘
                │
                ▼
           BRONZE LAYER
                │
        ┌───────┴────────┐
        │                │
        ▼                ▼
yellow_trips_raw   ingestion_audit
        │                │
        └───────┬────────┘
                ▼
           SILVER LAYER
                │
      ┌─────────┼─────────────────┐
      ▼         ▼                 ▼
   staging   cleaned          zone mapping
                │
                ▼
      incremental trip fact
                │
      ┌─────────┴──────────┐
      ▼                    ▼
taxi_zone_current      taxi_zone_history
                           │
                         SCD2
                │
                ▼
             GOLD LAYER
                │
       ┌────────┼──────────────┐
       ▼        ▼              ▼
   fct_trips  dim_taxi_zone  analytics
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
             daily_analytics   hourly_demand_metrics
                    │
                    └─────────┬──────────┘
                              ▼
                  Urban Mobility Dashboard
```

---

## What the pipeline does

A new monthly file can be dropped into the Databricks source Volume:

```text
yellow_tripdata_2026-05.parquet
yellow_tripdata_2026-06.parquet
yellow_tripdata_2026-07.parquet
```

The pipeline automatically:

1. Discovers available Yellow Taxi files.
2. Checks the ingestion audit table for previously processed files.
3. Processes only new files.
4. Adds ingestion metadata.
5. Generates a deterministic SHA-256 record fingerprint.
6. MERGEs records into the Bronze Delta table.
7. Transforms and validates data through dbt.
8. Incrementally updates Silver models.
9. Maintains current and historical taxi-zone dimensions.
10. Builds Gold analytics tables.
11. Runs dbt data-quality tests.
12. Refreshes the published Databricks dashboard.

---

## Technology Stack

| Technology                 | Purpose                                       |
| -------------------------- | --------------------------------------------- |
| Python                     | Ingestion and pipeline logic                  |
| PySpark                    | Distributed data processing                   |
| Databricks                 | Compute, notebooks, Jobs and SQL              |
| Unity Catalog              | Catalog and schema management                 |
| Delta Lake                 | Transactional storage and MERGE operations    |
| SQL                        | Transformation and analytics                  |
| dbt Core                   | Data transformation, dependencies and testing |
| dbt-databricks             | Databricks adapter for dbt                    |
| Jinja                      | Dynamic dbt SQL configuration                 |
| Git / GitHub               | Source control and project management         |
| Databricks AI/BI Dashboard | Analytics visualization                       |

---

## Data Source

The project uses the **NYC Taxi & Limousine Commission (TLC) Trip Record Data**.

The primary dataset used is:

**Yellow Taxi Trip Record Data**

Monthly Parquet files were used for the project, initially covering January–April 2026 and subsequently extended through July 2026 for end-to-end pipeline testing.

A taxi-zone lookup dataset is also used to enrich trips with:

* Borough
* Zone
* Service zone

The source data is treated as an external input to the platform and is not modified directly.

---

# Data Architecture

## Bronze

Bronze preserves the source data with ingestion metadata.

### `bronze.yellow_trips_raw`

Contains the original Yellow Taxi trip records plus:

```text
_record_hash
_source_file
_source_month
_ingested_at
_ingestion_batch_id
```

The raw source values are retained so downstream validation or reprocessing can occur without losing the original ingestion payload.

### `bronze.ingestion_audit`

Tracks successfully processed input files:

```text
source_file
source_month
status
record_count
processed_at
```

This table provides the file-level ingestion control mechanism.

---

# Silver

Silver contains cleaned, standardized and enriched data.

### `stg_yellow_trips`

dbt staging model over the Bronze source.

### `int_yellow_trips_cleaned`

Applies business/data-quality rules including:

* Pickup and dropoff timestamps must exist.
* Trip distance must be greater than zero.
* Dropoff must occur after pickup.
* Total amount must not be negative.
* Passenger count must be greater than zero.

Derived fields include:

```text
trip_duration_minutes
pickup_date
pickup_hour
```

Invalid records are filtered downstream rather than deleting information from Bronze.

### `int_yellow_trip_taxi_zone_map`

Enriches trips using pickup and dropoff taxi-zone attributes.

### `fct_int_yellow_trip_enriched`

Incrementally maintained trip-level Silver fact.

Configuration:

```text
materialized = incremental
incremental_strategy = merge
unique_key = _record_hash
```

The model uses `_source_file` to identify previously processed files, allowing new files to be added without depending on sequential month arrival.

---

# Idempotency

The pipeline provides protection at two levels.

## File-level idempotency

The ingestion audit table records successfully processed source files.

For example:

```text
yellow_tripdata_2026-05.parquet
```

is recorded using its full source path.

When the ingestion notebook runs again, the file is detected as already processed and skipped.

```text
First run:
May → PROCESS

Second run:
May → SKIP
```

## Record-level protection

Every source record receives a deterministic SHA-256 fingerprint:

```text
_record_hash
```

The Bronze Delta table uses this fingerprint during MERGE operations.

This prevents identical source records from being inserted repeatedly.

Together:

```text
File-level control
        +
Record-level protection
        =
Idempotent ingestion
```

---

# Incremental Processing

The pipeline is designed for monthly batch arrivals.

Example:

```text
January
February
March
April
```

Initial processing loads the available data.

Later:

```text
May arrives
```

Only May is newly ingested.

Later:

```text
June arrives
```

Only June is newly ingested.

The ingestion process does not require hard-coded month lists.

The same mechanism also supports a late-arriving file:

```text
January
February
March
April
June

        ↓

May arrives later

        ↓

May is detected as a new source file
```

This avoids relying on:

```text
MAX(_source_month)
```

as the sole indicator of new data.

---

# Current-State Dimension

## `silver.taxi_zone_current`

Maintains the latest known taxi-zone attributes.

Business key:

```text
LocationID
```

The current-state table uses MERGE semantics so changed attributes overwrite the current version.

This is useful for consumers that only need the latest taxi-zone information.

---

# Slowly Changing Dimension Type 2

## `silver.taxi_zone_history`

Taxi-zone history is maintained using a dbt Snapshot with the **check strategy**.

Tracked attributes:

```text
Borough
Zone
service_zone
```

Example:

```text
LocationID = 161

Old:
Zone = Midtown Center

New:
Zone = Midtown Center - Updated
```

The historical record remains available while the new version becomes the current record.

Metadata columns include:

```text
valid_from
valid_till
dbt_scd_id
```

Current version:

```text
valid_till IS NULL
```

Historical version:

```text
valid_till IS NOT NULL
```

A separate test/simulation source was used to safely demonstrate source changes without modifying the original raw reference dataset.

---

# Gold Layer

Gold contains analytics-ready models intended for business and reporting use.

## `gold.fct_trips`

Trip-level analytics fact table.

Contains cleaned and enriched trip-level information including:

* Trip timestamps
* Duration
* Distance
* Passenger information
* Pickup/dropoff zones
* Boroughs
* Service zones
* Payment information
* Fare components
* Total revenue

## `gold.dim_taxi_zone`

Current taxi-zone dimension used by analytical consumers.

## `gold.daily_analytics`

Daily metrics by pickup date and pickup zone.

Grain:

```text
pickup_date + PULocationID
```

Metrics include:

```text
total_trips
total_revenue
total_tips
avg_trip_revenue
avg_trip_distance
avg_trip_duration_minutes
avg_passenger_count
```

## `gold.hourly_demand_metrics`

Hourly demand metrics by pickup date, hour and pickup zone.

Grain:

```text
pickup_date + pickup_hour + PULocationID
```

This enables analysis of demand patterns throughout the day.

---

# Data Quality

dbt tests are used to validate model contracts.

Examples include:

```text
not_null
unique
```

and custom singular tests for composite grains such as:

```text
daily_analytics:
pickup_date + PULocationID

hourly_demand_metrics:
pickup_date + pickup_hour + PULocationID
```

A successful pipeline requires the associated dbt build/tests to pass before downstream orchestration continues.

---

# Orchestration

The complete workflow is orchestrated through a Databricks Job.

```text
┌─────────────────────────────┐
│ ingest_yellow_taxi          │
│ Notebook Task               │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ dbt_build                   │
│ dbt build                   │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│ refresh_dashboard           │
│ Dashboard Task              │
└─────────────────────────────┘
```

The result is an end-to-end workflow:

```text
New File
   ↓
Ingestion
   ↓
Bronze
   ↓
Silver
   ↓
Gold
   ↓
Data Quality Tests
   ↓
Dashboard Refresh
```

---

# Dashboard

The project includes an **Urban Mobility Intelligence** dashboard built on Gold-layer datasets.

Example analytical views include:

### KPI Summary

* Total trips
* Total revenue
* Average trip distance
* Average trip duration
* Average trip revenue

### Daily Trip Volume

Daily trend of total taxi trips.

### Top Pickup Zones

Pickup zones ranked by trip volume.

### Revenue by Borough

Revenue distribution across boroughs.

### Demand by Hour

Hourly trip-demand distribution.

Because the dashboard is downstream of the pipeline, newly ingested monthly data becomes visible after a successful workflow execution and dashboard refresh.

---

# Repository Structure

```text
urban-mobility-data-platform/
│
├── ingestion/
│
├── pyspark/
│   └── notebooks/
│
├── sql/
│
├── dbt/
│   └── urban_mobility/
│       ├── models/
│       │   ├── staging/
│       │   ├── intermediate/
│       │   └── gold/
│       │
│       ├── snapshots/
│       ├── macros/
│       ├── seeds/
│       ├── tests/
│       └── dbt_project.yml
│
├── config/
├── docs/
├── data/
│
├── tests/
├── README.md
├── requirements.txt
└── .gitignore
```

Credentials and local dbt profiles are intentionally excluded from version control.

---

# Running the Project

## 1. Prepare source data

Place the monthly Yellow Taxi Parquet files in the configured Databricks Volume.

Example:

```text
/Volumes/urban-mobility-data-platform-dev/source/data/
```

## 2. Run ingestion

Execute the Bronze ingestion notebook.

The notebook automatically:

```text
discovers files
      ↓
checks ingestion_audit
      ↓
processes new files
      ↓
MERGEs into Bronze
      ↓
records successful ingestion
```

## 3. Run dbt

From the dbt project:

```bash
dbt build
```

For individual models:

```bash
dbt run --select model_name
```

For tests:

```bash
dbt test --select model_name
```

For the SCD2 snapshot:

```bash
dbt snapshot --select taxi_zone_history
```

## 4. Production-style orchestration

The Databricks Job executes:

```text
Bronze ingestion
      ↓
dbt build
      ↓
Dashboard refresh
```

---

# Idempotency Demonstration

The pipeline was explicitly tested by repeatedly executing the same ingestion process.

Example:

```text
May uploaded
    ↓
May ingested
    ↓
Run pipeline again
    ↓
May detected as already processed
    ↓
May skipped
```

Bronze record counts were cross-validated against Silver and Gold outputs to verify that new monthly data flowed through the complete pipeline without creating duplicate data.

---

# Example End-to-End Scenario

Suppose the platform currently contains data through April:

```text
Jan → Apr
```

A new file is uploaded:

```text
yellow_tripdata_2026-05.parquet
```

The workflow executes:

```text
May file
   ↓
Bronze ingestion
   ↓
Silver transformations
   ↓
Incremental fact MERGE
   ↓
Gold models
   ↓
dbt tests
   ↓
Dashboard refresh
```

The dashboard then reflects the additional May data.

Uploading June and July follows exactly the same mechanism without modifying the pipeline code.

---

# Engineering Concepts Demonstrated

This project intentionally focuses on practical data-engineering concepts rather than simply demonstrating syntax.

### Data Engineering

* Batch ingestion
* Data lake architecture
* Medallion architecture
* Distributed processing
* Delta Lake
* Incremental pipelines
* MERGE operations
* Data-quality validation
* Deduplication
* Idempotency
* Late-arriving data
* SCD Type 2
* Data lineage and model dependencies
* Workflow orchestration
* Analytics serving

### dbt

* Sources
* `source()`
* `ref()`
* Incremental models
* Jinja
* Snapshots
* Tests
* Model contracts
* Custom schema generation
* Dependency-aware builds

### Databricks

* Unity Catalog
* Volumes
* Delta tables
* SQL Warehouse
* Notebooks
* Git integration
* Jobs
* Dashboard refresh workflows

---

# Design Decisions

## Why SHA-256 record hashes?

The source dataset does not provide a simple universal trip identifier.

A deterministic hash provides a reproducible fingerprint of the source record and can be used for exact-duplicate detection and idempotent MERGE operations.

It is treated as a **record fingerprint**, not as an officially guaranteed business identifier.

## Why keep raw invalid records in Bronze?

Bronze acts as the source-preservation layer.

Data-quality rules are applied downstream so that the original ingested data remains available for investigation and reprocessing.

## Why SCD Type 2?

Taxi-zone reference attributes can change over time.

Current-state consumers need the latest values, while historical analysis may need to know what the attributes looked like at a previous point in time.

The project therefore demonstrates both:

```text
Current state → taxi_zone_current
History       → taxi_zone_history
```

## Why batch rather than streaming?

The selected NYC TLC Yellow Taxi data is provided as monthly trip-record files.

The project therefore intentionally implements **incremental batch processing** rather than pretending that the source is real-time streaming data.

---

# Future Improvements

Possible production extensions include:

* Automated source-file landing from cloud object storage
* Schema evolution handling
* File-content checksums/version tracking
* Alerting and failure notifications
* More sophisticated data observability
* Parameterized backfills
* Environment-specific deployment
* CI/CD for dbt
* Additional analytical dimensions
* More granular operational monitoring

These are intentionally outside the current project scope.

---

# Project Goal

The purpose of this project is to demonstrate an end-to-end data-engineering workflow where a new source file can move through the complete platform:

```text
SOURCE
  ↓
BRONZE
  ↓
SILVER
  ↓
GOLD
  ↓
QUALITY CHECKS
  ↓
DASHBOARD
```

while maintaining:

```text
Incremental Processing
        +
Idempotency
        +
Data Quality
        +
Historical Tracking
        +
Orchestration
        +
Analytics
```

---

## Author

Built as a hands-on data-engineering portfolio project using NYC TLC Yellow Taxi data.
