# Urban Mobility Data Intelligence Platform
# 🚕 Urban Mobility Data Intelligence Platform

A production-style **data engineering platform** for ingesting, cleaning, validating, incrementally processing, and modeling large-scale urban mobility data into reliable analytics-ready datasets.

The project is designed around real-world data engineering problems rather than a simple ETL demonstration: **duplicate records, changing source data, incremental loads, historical tracking, data-quality failures, idempotent processing, and repeatable pipeline execution.**

> **Project Status:** In Development
> **Primary Goal:** Build an end-to-end, production-style data pipeline suitable for portfolio and interview demonstration.

---

## 🎯 Project Objective

Urban mobility generates large volumes of trip and operational data. A useful analytics platform must do more than simply load this data.

It needs to:

* ingest raw source data reliably
* preserve the original source layer
* validate and clean incoming records
* handle duplicate and late-arriving records
* process only new or changed data
* synchronize source changes with target tables
* preserve historical dimension changes
* produce trusted analytical datasets
* provide data-quality checks and pipeline observability
* support repeatable and idempotent processing

This project implements those concepts using a modern **lakehouse-style architecture**.

---

# 🏗️ Architecture

```text
                    PUBLIC MOBILITY DATA
                            │
                            ▼
                    PYTHON INGESTION
                            │
                            ▼
                  ┌──────────────────┐
                  │   BRONZE / RAW   │
                  │  Databricks /    │
                  │    Delta Lake    │
                  └────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │     PYSPARK      │
                  │                  │
                  │ Schema Validation│
                  │ Cleaning         │
                  │ Deduplication    │
                  │ Transformations  │
                  └────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │ SILVER / CURATED │
                  │      Delta       │
                  └────────┬─────────┘
                           │
                     MERGE / SCD2
                           │
                           ▼
                  ┌──────────────────┐
                  │       dbt        │
                  │   SQL + Jinja    │
                  │                  │
                  │ Models           │
                  │ Tests            │
                  │ Documentation    │
                  └────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │  GOLD / BUSINESS │
                  │     MODELS       │
                  └────────┬─────────┘
                           │
                           ▼
                 DATABRICKS SQL / BI
                           │
                           ▼
                       ANALYTICS
```

---

# 🧰 Technology Stack

| Technology        | Purpose                                                    |
| ----------------- | ---------------------------------------------------------- |
| **Python**        | Data ingestion, utilities, validation, pipeline logic      |
| **SQL**           | Data transformation, analytical queries, incremental logic |
| **PySpark**       | Distributed data processing and transformation             |
| **Apache Spark**  | Large-scale data processing engine                         |
| **Databricks**    | Lakehouse compute, Spark execution and data platform       |
| **Delta Lake**    | Reliable storage and transactional table layer             |
| **Unity Catalog** | Data governance and cataloging                             |
| **dbt Core**      | SQL transformation, modeling and testing                   |
| **Jinja**         | Dynamic/reusable SQL generation inside dbt                 |
| **Git/GitHub**    | Version control and project management                     |

---

# 📊 Data Source

The primary source is the **NYC Taxi & Limousine Commission (TLC) Trip Record Data**.

The dataset contains trip-level information such as pickup/drop-off timestamps and locations, trip distance, fares, payment information, and passenger counts.

The TLC publishes the trip data as Parquet files and updates the collection periodically.

**Important:** This project is designed as an **incremental batch-processing pipeline**, not as a real-time traffic streaming system. The source's publication cadence determines when new source data becomes available.

Data source:

**NYC Taxi & Limousine Commission — TLC Trip Record Data**

---

# 🔄 Pipeline Flow

## 1. Ingestion

Python retrieves incoming source files and registers ingestion metadata.

Example metadata:

```text
batch_id
source_file
ingestion_timestamp
source_period
record_count
```

The ingestion layer is designed to be repeatable and restartable.

---

## 2. Bronze Layer

Raw source data is stored with minimal transformation.

Example:

```text
mobility.bronze.trip_raw
```

The Bronze layer preserves source information and provides a recoverable landing point for downstream processing.

Typical metadata fields:

```text
_ingested_at
_batch_id
_source_file
```

---

## 3. Data Quality & Validation

Incoming records are validated before becoming trusted data.

Examples:

```text
✓ Required fields are present
✓ Correct data types
✓ Valid timestamps
✓ Valid trip distances
✓ Valid monetary values
✓ Valid location identifiers
✓ Duplicate detection
✓ Referential checks
```

Invalid records can be separated or quarantined instead of silently entering trusted datasets.

---

# 🧹 4. Data Cleaning & Transformation

PySpark is used to perform scalable transformations.

Typical operations include:

```text
Data type standardization
Null handling
Column normalization
Business-rule validation
Derived columns
Filtering invalid records
Joining reference data
```

The goal is to create a reliable Silver dataset.

---

# 🔁 5. Deduplication

Source systems can produce multiple records for the same business entity or event.

The pipeline uses window functions such as:

```sql
ROW_NUMBER() OVER (
    PARTITION BY business_key
    ORDER BY updated_at DESC
)
```

to identify the latest valid record.

Conceptually:

```text
Multiple source records
        ↓
PARTITION BY business key
        ↓
Order newest → oldest
        ↓
ROW_NUMBER()
        ↓
Keep rn = 1
```

This creates a deterministic source dataset before incremental synchronization.

---

# ⚡ 6. Incremental Processing

The pipeline is designed to avoid unnecessarily reprocessing the entire historical dataset.

Instead:

```text
Existing Target
      +
New / Changed Source Records
      ↓
Incremental Processing
      ↓
Target Update
```

This allows the pipeline to process only relevant new or changed data.

---

# 🔀 7. MERGE

After preparing the source dataset, Databricks `MERGE` logic synchronizes the curated target.

Conceptually:

```text
Source Record
      │
      ▼
Does business key exist?
      │
   ┌──┴──┐
   │     │
  YES    NO
   │     │
UPDATE  INSERT
```

The pipeline also avoids ambiguous merges by deduplicating source records before synchronization.

---

# 🕒 8. SCD Type 2

Selected dimensions maintain historical changes using **Slowly Changing Dimension Type 2** logic.

Example:

```text
vehicle_id | operator | valid_from | valid_to | is_current
-----------|----------|------------|----------|-----------
101        | Company A| 2026-01-01 | 2026-08-15 | 0
101        | Company B| 2026-08-15 | NULL       | 1
```

Instead of overwriting history, the system preserves previous versions.

This enables historical analysis such as:

> "Which operator was associated with this entity during a specific period?"

---

# 🥈 Silver Layer

Silver contains cleaned, validated and curated datasets.

Potential datasets include:

```text
mobility.silver.trips
mobility.silver.trip_events
mobility.silver.vehicle_history
mobility.silver.location_reference
```

The exact model names may evolve during development.

Silver represents:

> **Trusted data suitable for downstream transformation.**

---

# 🥇 Gold Layer

Gold contains business-oriented datasets designed for analytics.

Examples:

```text
mobility.gold.daily_trip_metrics
mobility.gold.hourly_demand
mobility.gold.zone_performance
mobility.gold.revenue_metrics
mobility.gold.trip_duration_metrics
```

Example metrics:

```text
Total Trips
Total Revenue
Average Trip Distance
Average Trip Duration
Demand by Hour
Demand by Location
Peak Periods
Payment Distribution
```

The Gold layer is intended to be consumed by analytics tools and SQL users.

---

# 🧱 dbt Transformation Layer

dbt is used primarily for SQL-based transformation and analytical modeling.

Example model:

```sql
SELECT
    DATE(pickup_datetime) AS trip_date,
    pickup_zone,
    COUNT(*) AS total_trips,
    AVG(trip_distance) AS avg_distance,
    SUM(fare_amount) AS total_revenue
FROM {{ ref('silver_trips') }}
GROUP BY
    DATE(pickup_datetime),
    pickup_zone
```

dbt provides:

```text
Models
Sources
ref()
Tests
Jinja
Macros
Documentation
Incremental models
Dependency management
```

The dbt project is developed locally using dbt Core and executes transformations against Databricks.

---

# 🧪 Data Quality

The project treats data quality as part of the pipeline rather than an afterthought.

Example checks:

```text
Primary key uniqueness
Required fields not null
Accepted values
Valid numeric ranges
Valid timestamps
Referential integrity
Duplicate detection
```

Example dbt test:

```yaml
columns:
  - name: trip_id
    tests:
      - unique
      - not_null
```

The pipeline should fail or quarantine problematic data where appropriate instead of silently producing unreliable analytical results.

---

# 🔄 Idempotency

A major engineering requirement of the project is **idempotent processing**.

If the same input batch is processed twice:

```text
Run 1 → correct target state
Run 2 → same correct target state
```

The second execution should not create:

```text
duplicate trips
duplicate revenue
duplicate dimension records
```

This is achieved through deterministic record identification, deduplication and incremental synchronization logic.

---

# 🛠️ Failure & Recovery Scenarios

The pipeline is designed to demonstrate how production systems handle failure.

Examples:

### Duplicate source records

```text
Detect duplicates
        ↓
Keep latest valid record
        ↓
Continue processing
```

### Invalid records

```text
Validation failure
        ↓
Quarantine / reject
        ↓
Continue valid records
```

### Failed batch

```text
Pipeline failure
      ↓
Identify missing batch
      ↓
Backfill
      ↓
Reprocess safely
```

### Repeated execution

```text
Same batch
    ↓
Idempotent processing
    ↓
No duplicate target records
```

---

# 📁 Repository Structure

```text
urban-mobility-data-platform/
│
├── README.md
│
├── ingestion/
│   ├── ingest.py
│   └── config.py
│
├── pyspark/
│   ├── bronze/
│   ├── silver/
│   ├── transformations/
│   └── utils/
│
├── sql/
│   ├── exploratory/
│   ├── validation/
│   └── analytics/
│
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── snapshots/
│   ├── macros/
│   ├── tests/
│   └── seeds/
│
├── tests/
│
├── docs/
│   ├── architecture/
│   ├── data_dictionary/
│   └── diagrams/
│
├── config/
│
└── requirements.txt
```

---

# 📈 Analytics

Gold datasets can be queried through **Databricks SQL** and used to create analytical visualizations.

Example dashboard sections:

### Mobility Demand

* Trips by hour
* Trips by day
* Trips by location
* Peak demand periods

### Operational Performance

* Average trip duration
* Average trip distance
* Revenue
* Trip volume trends

### Data Reliability

* Records ingested
* Records rejected
* Duplicate records
* Late-arriving records
* Pipeline execution metrics

The analytics layer sits **on top of the Gold models**, keeping business-facing queries separated from raw processing logic.

---

# 🔐 Data Architecture Principles

The project follows these principles:

### Separation of layers

```text
Bronze → source-oriented
Silver → trusted/curated
Gold   → business-oriented
```

### Incremental over unnecessary full refresh

Process new and changed data whenever possible.

### Data quality by design

Validate before publishing trusted datasets.

### Idempotent execution

A retry should not corrupt the target.

### Historical preservation

Use SCD Type 2 where business history matters.

### Reproducibility

Everything possible is represented as code and tracked in Git.

### Clear responsibility between tools

```text
Python
→ ingestion / utilities

PySpark
→ scalable data engineering

Databricks / Delta
→ lakehouse processing and storage

SQL
→ analytical transformation

dbt + Jinja
→ modular SQL modeling and testing
```

---

# 🚀 Future Enhancements

Potential future improvements include:

* Pipeline orchestration
* Automated scheduling
* More sophisticated data observability
* Additional mobility sources
* Streaming ingestion
* More advanced performance optimization
* CI/CD for dbt
* Automated deployment
* Advanced analytics and forecasting

These are deliberately outside the initial implementation scope so the core pipeline remains reliable and explainable.

---

# 🎯 What This Project Demonstrates

This project is intended to demonstrate practical knowledge of:

```text
SQL
Python
PySpark
Apache Spark
Databricks
Delta Lake
Unity Catalog
dbt
Jinja
Window Functions
CTEs
MERGE
Incremental Loads
SCD Type 2
Data Quality
Data Validation
Deduplication
Idempotency
Error Handling
Git
```

More importantly, it demonstrates the ability to reason about a complete data pipeline:

```text
How does data arrive?
        ↓
How is it stored?
        ↓
How do we validate it?
        ↓
How do we handle bad data?
        ↓
How do we remove duplicates?
        ↓
How do we process only changes?
        ↓
How do we maintain history?
        ↓
How do we transform it?
        ↓
How do we expose it for analytics?
        ↓
How do we safely rerun the pipeline?
```

---

# 📌 Project Philosophy

This project intentionally focuses on **engineering decisions rather than technology count**.

The goal is not to use every modern data tool.

The goal is to build a pipeline that is:

**Reliable → Repeatable → Incremental → Testable → Explainable**

and to be able to explain why each architectural decision was made.

---

## Author

Built as a hands-on Data Engineering portfolio project.

**Primary technologies:** Python · SQL · PySpark · Databricks · Delta Lake · dbt · Jinja · Git

