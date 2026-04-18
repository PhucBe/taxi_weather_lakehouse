-- =========================================================
-- BRONZE DATA QUALITY CHECKS
-- ENGINE: DUCKDB
-- PURPOSE:
--   - QUICK VALIDATION FOR BRONZE PARQUET LAYER
--   - MANUAL INSPECTION AFTER SPARK RAW -> BRONZE
-- =========================================================


-- =========================================================
-- 0) SOURCE VIEWS
-- =========================================================

CREATE OR REPLACE VIEW taxi_bronze AS
SELECT *
FROM read_parquet('data/bronze/taxi_trips_raw/pickup_year=*/pickup_month=*/*.parquet');

CREATE OR REPLACE VIEW weather_bronze AS
SELECT *
FROM read_parquet('data/bronze/weather_raw/weather_year=*/weather_month=*/*.parquet');

CREATE OR REPLACE VIEW zone_bronze AS
SELECT *
FROM read_parquet('data/bronze/zone_lookup/*.parquet');

CREATE OR REPLACE VIEW taxi_raw AS
SELECT *
FROM read_csv_auto('data/raw/taxi/flat/year=*/month=*/*.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW weather_raw AS
SELECT *
FROM read_csv_auto('data/raw/weather/flat/start_date=*/end_date=*/*.csv', HEADER = TRUE);

CREATE OR REPLACE VIEW zone_raw AS
SELECT *
FROM read_csv_auto('data/raw/zone_lookup/*.csv', HEADER = TRUE);


-- =========================================================
-- 1) RAW VS BRONZE ROW COUNT
-- =========================================================

SELECT
    'taxi' AS dataset,
    (SELECT COUNT(*) FROM taxi_raw) AS raw_count,
    (SELECT COUNT(*) FROM taxi_bronze) AS bronze_count
UNION ALL
SELECT
    'weather' AS dataset,
    (SELECT COUNT(*) FROM weather_raw) AS raw_count,
    (SELECT COUNT(*) FROM weather_bronze) AS bronze_count
UNION ALL
SELECT
    'zone' AS dataset,
    (SELECT COUNT(*) FROM zone_raw) AS raw_count,
    (SELECT COUNT(*) FROM zone_bronze) AS bronze_count;


-- =========================================================
-- 2) PREVIEW / SCHEMA CHECK
-- =========================================================

DESCRIBE SELECT * FROM taxi_bronze;
DESCRIBE SELECT * FROM weather_bronze;
DESCRIBE SELECT * FROM zone_bronze;

SELECT *
FROM taxi_bronze
LIMIT 5;

SELECT *
FROM weather_bronze
LIMIT 5;

SELECT *
FROM zone_bronze
LIMIT 5;


-- =========================================================
-- 3) TAXI BRONZE CHECKS
-- =========================================================

-- 3.1 ROW COUNT BY MONTH PARTITION
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM taxi_bronze
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

-- 3.2 MIN / MAX PICKUP-DROPOFF DATETIME
SELECT
    MIN(tpep_pickup_datetime) AS min_pickup_ts,
    MAX(tpep_pickup_datetime) AS max_pickup_ts,
    MIN(tpep_dropoff_datetime) AS min_dropoff_ts,
    MAX(tpep_dropoff_datetime) AS max_dropoff_ts
FROM taxi_bronze;

-- 3.3 NULL CHECK FOR TECHNICAL COLUMNS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS source_file_nulls,
    SUM(CASE WHEN bronze_loaded_at IS NULL THEN 1 ELSE 0 END) AS bronze_loaded_at_nulls
FROM taxi_bronze;

-- 3.4 BASIC NEGATIVE VALUE REVIEW
SELECT
    SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_trip_distance,
    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare_amount,
    SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total_amount
FROM taxi_bronze;

-- 3.5 DISTINCT PARTITION COVERAGE
SELECT DISTINCT
    pickup_year,
    pickup_month
FROM taxi_bronze
ORDER BY pickup_year, pickup_month;

-- 3.6 RECORD COUNT BY SOURCE FILE
SELECT
    source_file,
    COUNT(*) AS row_count
FROM taxi_bronze
GROUP BY source_file
ORDER BY row_count DESC, source_file;


-- =========================================================
-- 4) WEATHER BRONZE CHECKS
-- =========================================================

-- 4.1 ROW COUNT BY MONTH PARTITION
SELECT
    weather_year,
    weather_month,
    COUNT(*) AS row_count
FROM weather_bronze
GROUP BY weather_year, weather_month
ORDER BY weather_year, weather_month;

-- 4.2 MIN / MAX DATE
SELECT
    MIN(date) AS min_date,
    MAX(date) AS max_date
FROM weather_bronze;

-- 4.3 NULL CHECK
SELECT
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS date_nulls,
    SUM(CASE WHEN weather_year IS NULL THEN 1 ELSE 0 END) AS weather_year_nulls,
    SUM(CASE WHEN weather_month IS NULL THEN 1 ELSE 0 END) AS weather_month_nulls,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS source_file_nulls,
    SUM(CASE WHEN bronze_loaded_at IS NULL THEN 1 ELSE 0 END) AS bronze_loaded_at_nulls
FROM weather_bronze;

-- 4.4 DUPLICATE DATE CHECK
SELECT
    date,
    COUNT(*) AS duplicate_count
FROM weather_bronze
GROUP BY date
HAVING COUNT(*) > 1
ORDER BY date;

-- 4.5 MISSING DATE CHECK
WITH calendar AS (
    SELECT *
    FROM generate_series(
        (SELECT MIN(date) FROM weather_bronze),
        (SELECT MAX(date) FROM weather_bronze),
        INTERVAL 1 DAY
    ) AS t(expected_date)
)
SELECT
    c.expected_date
FROM calendar c
LEFT JOIN weather_bronze w
    ON c.expected_date = w.date
WHERE w.date IS NULL
ORDER BY c.expected_date;


-- =========================================================
-- 5) ZONE BRONZE CHECKS
-- =========================================================

-- 5.1 ROW COUNT AND DISTINCT LOCATION_ID
SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT location_id) AS distinct_location_id
FROM zone_bronze;

-- 5.2 NULL CHECK
SELECT
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS location_id_nulls,
    SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS borough_nulls,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS zone_nulls,
    SUM(CASE WHEN service_zone IS NULL THEN 1 ELSE 0 END) AS service_zone_nulls
FROM zone_bronze;

-- 5.3 DUPLICATE NATURAL KEY CHECK
SELECT
    location_id,
    borough,
    zone,
    service_zone,
    COUNT(*) AS duplicate_count
FROM zone_bronze
GROUP BY location_id, borough, zone, service_zone
HAVING COUNT(*) > 1
ORDER BY location_id, borough, zone, service_zone;

-- 5.4 RECORD COUNT BY BOROUGH
SELECT
    borough,
    COUNT(*) AS row_count
FROM zone_bronze
GROUP BY borough
ORDER BY row_count DESC, borough;