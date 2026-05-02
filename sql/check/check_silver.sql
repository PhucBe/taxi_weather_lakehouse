
-- 0) LOAD SILVER DATA AS VIEWS
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


-- 1) OVERVIEW
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

ORDER BY dataset_name;


-- 2) TAXI SILVER COVERAGE BY MONTH
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


-- 3) WEATHER SILVER COVERAGE BY MONTH
SELECT
    weather_year,
    weather_month,
    COUNT(*) AS weather_day_count
FROM silver_weather_daily
GROUP BY
    weather_year,
    weather_month
ORDER BY
    weather_year,
    weather_month;


-- 4) SILVER OUT-OF-SCOPE CHECKS
-- 4.1) TAXI OUT-OF-SCOPE SUMMARY
SELECT
    SUM(
        CASE
            WHEN pickup_date < DATE '2023-01-01'
              OR pickup_date > DATE '2023-03-31'
            THEN 1 ELSE 0
        END
    ) AS out_of_scope_taxi_date_rows,
    SUM(
        CASE
            WHEN pickup_year <> 2023
              OR pickup_month NOT IN (1, 2, 3)
            THEN 1 ELSE 0
        END
    ) AS out_of_scope_taxi_partition_rows
FROM silver_taxi_trips;

-- 4.2) TAXI OUT-OF-SCOPE DETAILS
SELECT
    pickup_date,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM silver_taxi_trips
WHERE pickup_date < DATE '2023-01-01'
   OR pickup_date > DATE '2023-03-31'
   OR pickup_year <> 2023
   OR pickup_month NOT IN (1, 2, 3)
GROUP BY
    pickup_date,
    pickup_year,
    pickup_month
ORDER BY
    pickup_date,
    pickup_year,
    pickup_month;

-- 4.3) WEATHER OUT-OF-SCOPE SUMMARY
SELECT
    SUM(
        CASE
            WHEN weather_date < DATE '2023-01-01'
              OR weather_date > DATE '2023-03-31'
            THEN 1 ELSE 0
        END
    ) AS out_of_scope_weather_date_rows,
    SUM(
        CASE
            WHEN weather_year <> 2023
              OR weather_month NOT IN (1, 2, 3)
            THEN 1 ELSE 0
        END
    ) AS out_of_scope_weather_partition_rows
FROM silver_weather_daily;


-- 4.4) WEATHER OUT-OF-SCOPE DETAILS
SELECT
    weather_date,
    weather_year,
    weather_month,
    COUNT(*) AS row_count
FROM silver_weather_daily
WHERE weather_date < DATE '2023-01-01'
   OR weather_date > DATE '2023-03-31'
   OR weather_year <> 2023
   OR weather_month NOT IN (1, 2, 3)
GROUP BY
    weather_date,
    weather_year,
    weather_month
ORDER BY
    weather_date,
    weather_year,
    weather_month;


-- 5) TAXI CORE DATA QUALITY CHECKS
SELECT
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS trip_id_nulls,
    SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_datetime_nulls,
    SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS dropoff_datetime_nulls,
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN pickup_hour IS NULL THEN 1 ELSE 0 END) AS pickup_hour_nulls,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END) AS pickup_location_id_nulls,
    SUM(CASE WHEN dropoff_location_id IS NULL THEN 1 ELSE 0 END) AS dropoff_location_id_nulls,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS source_file_nulls,
    SUM(CASE WHEN bronze_loaded_at IS NULL THEN 1 ELSE 0 END) AS bronze_loaded_at_nulls,
    SUM(CASE WHEN silver_loaded_at IS NULL THEN 1 ELSE 0 END) AS silver_loaded_at_nulls,
    SUM(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 ELSE 0 END) AS bad_time_order_rows,
    SUM(CASE WHEN trip_duration_minutes <= 0 THEN 1 ELSE 0 END) AS non_positive_duration_rows,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN pickup_hour <> HOUR(pickup_datetime) THEN 1 ELSE 0 END) AS bad_pickup_hour_rows
FROM silver_taxi_trips;


-- 6) WEATHER CORE DATA QUALITY CHECKS
SELECT
    SUM(CASE WHEN weather_date IS NULL THEN 1 ELSE 0 END) AS weather_date_nulls,
    SUM(CASE WHEN weather_year IS NULL THEN 1 ELSE 0 END) AS weather_year_nulls,
    SUM(CASE WHEN weather_month IS NULL THEN 1 ELSE 0 END) AS weather_month_nulls,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS source_file_nulls,
    SUM(CASE WHEN bronze_loaded_at IS NULL THEN 1 ELSE 0 END) AS bronze_loaded_at_nulls,
    SUM(CASE WHEN silver_loaded_at IS NULL THEN 1 ELSE 0 END) AS silver_loaded_at_nulls,
    SUM(CASE WHEN weather_year <> YEAR(weather_date) THEN 1 ELSE 0 END) AS bad_weather_year_rows,
    SUM(CASE WHEN weather_month <> MONTH(weather_date) THEN 1 ELSE 0 END) AS bad_weather_month_rows,
    SUM(CASE WHEN precipitation_sum > 0 AND is_rainy_day = FALSE THEN 1 ELSE 0 END) AS bad_rain_flag_rows,
    SUM(CASE WHEN precipitation_sum <= 0 AND is_rainy_day = TRUE THEN 1 ELSE 0 END) AS bad_non_rain_flag_rows,
    SUM(CASE WHEN snowfall_sum > 0 AND is_snowy_day = FALSE THEN 1 ELSE 0 END) AS bad_snow_flag_rows,
    SUM(CASE WHEN snowfall_sum <= 0 AND is_snowy_day = TRUE THEN 1 ELSE 0 END) AS bad_non_snow_flag_rows
FROM silver_weather_daily;


-- 7) ZONE CORE DATA QUALITY CHECKS
SELECT
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS location_id_nulls,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END) AS source_file_nulls,
    SUM(CASE WHEN bronze_loaded_at IS NULL THEN 1 ELSE 0 END) AS bronze_loaded_at_nulls,
    SUM(CASE WHEN silver_loaded_at IS NULL THEN 1 ELSE 0 END) AS silver_loaded_at_nulls,
    SUM(CASE WHEN TRIM(COALESCE(borough, '')) = '' THEN 1 ELSE 0 END) AS blank_borough_rows,
    SUM(CASE WHEN TRIM(COALESCE(zone, '')) = '' THEN 1 ELSE 0 END) AS blank_zone_rows,
    SUM(CASE WHEN TRIM(COALESCE(service_zone, '')) = '' THEN 1 ELSE 0 END) AS blank_service_zone_rows
FROM silver_zone_lookup;


-- 8) UNIQUENESS CHECKS
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


-- 9) TAXI VS WEATHER DATE COVERAGE
SELECT
    COUNT(*) AS taxi_dates_without_weather
FROM (
    SELECT DISTINCT t.pickup_date
    FROM silver_taxi_trips AS t
    LEFT JOIN silver_weather_daily AS w
        ON t.pickup_date = w.weather_date
    WHERE w.weather_date IS NULL
);


-- 10) TAXI VS ZONE LOOKUP COVERAGE
SELECT
    SUM(CASE WHEN p.location_id IS NULL THEN 1 ELSE 0 END) AS missing_pickup_zone_rows,
    SUM(CASE WHEN d.location_id IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_zone_rows
FROM silver_taxi_trips AS t
LEFT JOIN silver_zone_lookup AS p
    ON t.pickup_location_id = p.location_id
LEFT JOIN silver_zone_lookup AS d
    ON t.dropoff_location_id = d.location_id;


-- 11) DAILY ANALYTICS SNAPSHOT
SELECT
    pickup_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM silver_taxi_trips
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 31;


-- 12) PAYMENT MIX SNAPSHOT
SELECT
    pickup_date,
    payment_type_code,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM silver_taxi_trips
GROUP BY
    pickup_date,
    payment_type_code
ORDER BY
    pickup_date,
    trip_count DESC,
    payment_type_code
LIMIT 50;


-- 13) TOP PICKUP LOCATION IDS
SELECT
    pickup_location_id,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM silver_taxi_trips
GROUP BY pickup_location_id
ORDER BY trip_count DESC, total_revenue DESC
LIMIT 15;


-- 14) WEATHER SNAPSHOT
SELECT
    weather_date,
    temperature_mean,
    precipitation_sum,
    snowfall_sum,
    is_rainy_day,
    is_snowy_day
FROM silver_weather_daily
ORDER BY weather_date
LIMIT 31;


-- 15) ZONE LOOKUP SNAPSHOT
SELECT
    location_id,
    borough,
    zone,
    service_zone
FROM silver_zone_lookup
ORDER BY location_id
LIMIT 20;