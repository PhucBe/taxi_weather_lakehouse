-- =========================================================
-- CHECK_SILVER.SQL
-- ---------------------------------------------------------
-- Portfolio-ready SQL checks for the Silver layer
--
-- Scope:
-- - silver_taxi_trips
-- - silver_weather_daily
-- - silver_zone_lookup
-- - silver_taxi_trips_enriched
--
-- Purpose:
-- - confirm row counts and date coverage
-- - check key data quality rules
-- - verify enriched join completeness
-- - show a few analytics-ready outputs
--
-- Run in DuckDB:
--   .read sql/checks/check_silver.sql
-- =========================================================


-- =========================================================
-- 0) LOAD SILVER DATA AS VIEWS
-- =========================================================
CREATE OR REPLACE VIEW silver_taxi_trips AS
SELECT *
FROM read_parquet(
    'data/silver/taxi_trips/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW silver_weather_daily AS
SELECT *
FROM read_parquet(
    'data/silver/weather_daily/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW silver_zone_lookup AS
SELECT *
FROM read_parquet(
    'data/silver/zone_lookup/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW silver_taxi_trips_enriched AS
SELECT *
FROM read_parquet(
    'data/silver/taxi_trips_enriched/**/*.parquet',
    hive_partitioning = true
);


-- =========================================================
-- 1) OVERVIEW
-- ---------------------------------------------------------
-- Quick health check:
-- - row count
-- - min / max date
-- =========================================================
SELECT
    'silver_taxi_trips' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM silver_taxi_trips

UNION ALL

SELECT
    'silver_weather_daily' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(weather_date) AS min_date,
    MAX(weather_date) AS max_date
FROM silver_weather_daily

UNION ALL

SELECT
    'silver_zone_lookup' AS dataset_name,
    COUNT(*) AS row_count,
    NULL AS min_date,
    NULL AS max_date
FROM silver_zone_lookup

UNION ALL

SELECT
    'silver_taxi_trips_enriched' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM silver_taxi_trips_enriched
ORDER BY dataset_name;


-- =========================================================
-- 2) TAXI SILVER COVERAGE BY MONTH
-- ---------------------------------------------------------
-- Expectation:
-- - only 2023-01, 2023-02, 2023-03
-- =========================================================
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS trip_count
FROM silver_taxi_trips
GROUP BY
    pickup_year,
    pickup_month
ORDER BY
    pickup_year,
    pickup_month;


-- =========================================================
-- 3) CORE DATA QUALITY CHECKS
-- ---------------------------------------------------------
-- One compact query for the most important Silver rules
-- Expectation:
-- - all metrics should be 0
-- =========================================================
SELECT
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS trip_id_nulls,
    SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_datetime_nulls,
    SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS dropoff_datetime_nulls,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END) AS pickup_location_id_nulls,
    SUM(CASE WHEN dropoff_location_id IS NULL THEN 1 ELSE 0 END) AS dropoff_location_id_nulls,
    SUM(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 ELSE 0 END) AS bad_time_order_rows,
    SUM(CASE WHEN trip_duration_minutes <= 0 THEN 1 ELSE 0 END) AS non_positive_duration_rows
FROM silver_taxi_trips;


-- =========================================================
-- 4) UNIQUENESS CHECKS
-- ---------------------------------------------------------
-- Expectation:
-- - all duplicate counters should be 0
-- =========================================================
WITH taxi_dup AS (
    SELECT COUNT(*) AS duplicate_trip_id_rows
    FROM (
        SELECT trip_id
        FROM silver_taxi_trips
        GROUP BY trip_id
        HAVING COUNT(*) > 1
    )
),
weather_dup AS (
    SELECT COUNT(*) AS duplicate_weather_date_rows
    FROM (
        SELECT weather_date
        FROM silver_weather_daily
        GROUP BY weather_date
        HAVING COUNT(*) > 1
    )
),
zone_dup AS (
    SELECT COUNT(*) AS duplicate_location_id_rows
    FROM (
        SELECT location_id
        FROM silver_zone_lookup
        GROUP BY location_id
        HAVING COUNT(*) > 1
    )
)
SELECT
    taxi_dup.duplicate_trip_id_rows,
    weather_dup.duplicate_weather_date_rows,
    zone_dup.duplicate_location_id_rows
FROM taxi_dup, weather_dup, zone_dup;


-- =========================================================
-- 5) ENRICHED CONSISTENCY CHECKS
-- ---------------------------------------------------------
-- Expectation:
-- - taxi_rows = enriched_rows
-- - row_diff = 0
-- - missing_* = 0
-- =========================================================
SELECT
    (SELECT COUNT(*) FROM silver_taxi_trips) AS taxi_rows,
    (SELECT COUNT(*) FROM silver_taxi_trips_enriched) AS enriched_rows,
    (
        (SELECT COUNT(*) FROM silver_taxi_trips_enriched)
        - (SELECT COUNT(*) FROM silver_taxi_trips)
    ) AS row_diff,
    SUM(CASE WHEN pickup_location_id IS NOT NULL AND pickup_zone IS NULL THEN 1 ELSE 0 END) AS missing_pickup_zone_rows,
    SUM(CASE WHEN dropoff_location_id IS NOT NULL AND dropoff_zone IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_zone_rows,
    SUM(CASE WHEN pickup_date IS NOT NULL AND weather_date IS NULL THEN 1 ELSE 0 END) AS missing_weather_rows
FROM silver_taxi_trips_enriched;


-- =========================================================
-- 6) DAILY ANALYTICS SNAPSHOT
-- ---------------------------------------------------------
-- A compact downstream-ready view of Silver usability
-- =========================================================
SELECT
    pickup_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM silver_taxi_trips_enriched
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 31;


-- =========================================================
-- 7) BUSINESS SNAPSHOT
-- ---------------------------------------------------------
-- Two simple outputs that show enrichment is useful
-- =========================================================

-- 7A. Rainy vs non-rainy demand
SELECT
    is_rainy_day,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance
FROM silver_taxi_trips_enriched
GROUP BY is_rainy_day
ORDER BY is_rainy_day;

-- 7B. Top pickup zones
SELECT
    pickup_borough,
    pickup_zone,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM silver_taxi_trips_enriched
GROUP BY
    pickup_borough,
    pickup_zone
ORDER BY trip_count DESC, total_revenue DESC
LIMIT 15;