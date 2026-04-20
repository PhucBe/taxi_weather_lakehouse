# 🏗️ Data Warehouse & Mart Build: Production ETL Pipeline

An end-to-end data engineering pipeline that transforms raw APIs from NYC TLC Yellow Taxi Parquet, TLC Taxi Zone Lookup CSV and Open-Mateo Historical Weather API into a normalized star schema data warehouse, then builds analytical data marts.

![Data Pipeline Architecture](docs/screenshots/0.png)

---

## 🧾 Executive Summary (For Hiring Managers)

- ✅ **Pipeline scope:** Built a complete **ETL pipeline** from raw APIs to star schema warehouse to analytical marts  
- ✅ **Data modeling:** Designed a **star schema** with fact tables, dimensions, and bridge tables for many-to-many relationships  
- ✅ **ETL development:** Implemented **extract, transform, load** processes with idempotent operations and data quality checks  
- ✅ **Mart architecture:** Created **specialized data marts** (flat, skills, priority) with additive measures and incremental update patterns

---

## 🧩 Problem & Context

Raw NYC taxi trip data, taxi zone lookup data, and daily weather data come from separate sources — NYC TLC Yellow Taxi Parquet, TLC Taxi Zone Lookup CSV, and the Open-Meteo Historical Weather API — and are not ready for direct analytical use. Analysts need to answer:

- How does taxi demand change over time?
- Which zones generate the most trips and revenue?
- How do payment patterns vary by day and trip behavior?
- How does weather affect trip volume and revenue?

**Challenge:** Data teams need a single source of truth for taxi analytics because the raw data arrives at different grains (`trip`, `zone`, `date`), requires cleaning and enrichment, and is not structured for reliable BI queries or dashboarding. A layered pipeline is needed to standardize, validate, and transform the data into business-ready outputs.

**Solution:** Build an end-to-end Spark-based analytics pipeline that ingests data from APIs and source files, lands it in `raw`, transforms it through `bronze -> silver -> gold`, and publishes curated `serving` tables for downstream BI and dashboard use cases such as demand trend analysis, payment mix analysis, and weather impact analysis.

---

## 🧰 Tech Stack

- ☁️ **Storage**: Amazon S3 for raw landing and intermediate data storage
- 🐍 **Language**: Python for ingestion, validation, export, and pipeline scripting
- ⚡ **Processing Engine**: Apache Spark / PySpark for `raw -> bronze -> silver -> gold` transformations
- 🏛️ **Warehouse / Serving**: Amazon Redshift for raw-layer loading and curated serving tables
- 🐳 **Development Environment**: Docker / Docker Compose for local pipeline services
- 📊 **Dashboarding**: Metabase for demand, payment mix, and weather impact dashboards
- 🔄 **Orchestration**: Apache Airflow for scheduling, retry, dependency management, and backfill
- 📦 **Version Control**: Git / GitHub for code versioning and project collaboration

---

## 🏗️ Pipeline Architecture

![Data Pipeline Architecture](docs/screenshots/0.png)

The pipeline transforms job posting APIs from NYC TLC Yellow Taxi Parquet, TLC Taxi Zone Lookup CSV and Open-Mateo Historical Weather API into a normalized star schema data warehouse, then builds specialized analytical data marts. BI tools (Metabase, Power BI, Tableau) consume from both the warehouse and marts.

### Data Warehouse

The data warehouse implements a star schema with `dim_weather`, `dim_date`, `dim_zone`, and `fact_taxi_trips` tables.

![Data Warehouse Schema](docs/screenshots/1.png)

- **Purpose:** Star schema serving as single source of truth for analytical queries
- **Grain:** One row per job posting in the fact table (`fact_taxi_trips`)

### Daily Demand Mart

Denormalized table with all dimensions for ad-hoc queries.

![Flat Mart Schema](../../Resources/images/1_2_Flat_Mart.png)

- `Total trips`: Tổng số chuyến taxi trong khoảng thời gian được chọn

- `Total revenue`: Tổng doanh thu taxi trong khoảng thời gian được chọn

- `Average Revenue per Trip`: Doanh thu trung bình trên mỗi chuyến

- `Average Trip Duration (Minutes)`: Thời lượng trung bình của một chuyến taxi

![KPI](docs/screenshots/3.png)

- `Daily Trips vs Average Revenue per Trip`

![Daily Trips vs Average Revenue per Trip](docs/screenshots/4.png)

- `Average Trips and Revenue per Trip by Day of Week`
- `Daily Revenue Trend`

![Daily Revenue Trend](docs/screenshots/5.png)

- `Average Trip Distance vs Average Revenue per Trip`

![Average Trip Distance vs Average Revenue per Trip](docs/screenshots/6.png)

- `Top 10 Peak Demand Days`

![Top 10 Peak Demand Days](docs/screenshots/7.png)

---


## 💻 Data Engineering Skills Demonstrated

### ETL / ELT Pipeline Development

- **Multi-Source Ingestion**: Integrated data from `NYC TLC Yellow Taxi Parquet`, `TLC Taxi Zone Lookup CSV`, and `Open-Meteo Historical Weather API`
- **Raw Data Landing**: Built ingestion flow to collect, standardize, and land source data into `raw` storage before transformation
- **Layered Transformations**: Implemented medallion-style pipeline with `raw -> bronze -> silver -> gold -> serving`
- **Pipeline Modularity**: Separated ingestion, transformation, validation, and serving export into independent jobs for easier testing and debugging
- **Reusability / Backfill Readiness**: Designed pipeline so historical periods can be reprocessed consistently

### Spark Data Processing

- **Distributed Transformation**: Used `Apache Spark / PySpark` for large-scale batch processing across multiple pipeline layers
- **Schema Standardization**: Applied column selection, type casting, null handling, and field normalization during `raw -> bronze`
- **Business Logic Transformation**: Built cleaning and enrichment logic during `bronze -> silver`
- **Aggregation Engineering**: Created analytical aggregates during `silver -> gold` for dashboard-ready datasets
- **Serving Preparation**: Published curated outputs into a `serving` layer optimized for BI consumption

### Data Modeling

- **Fact Table Design**: Built central fact table `fact_taxi_trips` at grain `1 row = 1 taxi trip`
- **Dimension Modeling**: Designed supporting dimensions such as `dim_date`, `dim_zone`, and `dim_weather`
- **Analytical Mart Design**: Built business-facing marts including `mart_daily_demand`, `mart_daily_payment_mix`, and `mart_weather_impact`
- **Grain Management**: Integrated sources with different grains such as `trip`, `zone`, and `date`
- **Business-Oriented Modeling**: Structured outputs around key analytical questions: demand trend, payment behavior, and weather impact

### Data Enrichment & Integration

- **Geographic Enrichment**: Joined taxi trips with zone lookup data to add borough, zone, and service zone context
- **Temporal Enrichment**: Joined trip data with daily weather data by service date
- **Derived Metrics**: Computed fields such as trip duration, trip counts, revenue metrics, and daily weather flags
- **Categorical Standardization**: Normalized payment types for consistent downstream aggregation
- **Cross-Domain Analytics**: Combined mobility and weather datasets into a unified analytical model

### Data Quality & Validation

- **Validation by Layer**: Added checks for `raw`, `bronze`, `silver`, `gold`, and `serving`
- **Data Quality Rules**: Validated row counts, required columns, null behavior, and join completeness
- **Metric Validation**: Checked derived KPIs such as trip counts, revenue totals, and payment mix outputs
- **SQL Checks**: Used SQL-based validation queries to confirm that transformed outputs matched business expectations
- **Debugging Discipline**: Followed upstream-first debugging from raw data to marts instead of patching dashboard outputs

### Orchestration & Production Practices

- **Workflow Orchestration**: Used `Apache Airflow` to schedule and coordinate ingestion, transform, validation, and export tasks
- **Task Dependency Design**: Structured DAGs with clear job order, retry logic, and backfill capability
- **Operational Separation**: Kept business logic inside scripts and Spark jobs, with Airflow focused on orchestration only
- **Local Reproducibility**: Ran the project in a containerized development environment using `Docker / Docker Compose`
- **Portfolio-Ready Engineering**: Organized the project into clear layers, reusable scripts, validation steps, and BI-ready outputs

### Analytics Delivery

- **Dashboard Data Preparation**: Prepared serving tables specifically for dashboard use cases
- **KPI Enablement**: Supported analysis of daily demand, revenue trend, payment mix, rainy vs non-rainy demand, and snowy day behavior
- **Single Source of Truth**: Converted raw operational data into curated analytical datasets for consistent reporting
- **BI Consumption Layer**: Delivered stable outputs for tools like `Metabase`

