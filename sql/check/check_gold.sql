-- 0) LOAD PARQUET AS VIEWS
CREATE OR REPLACE VIEW silver_taxi_trips AS
SELECT *
FROM read_parquet(
    'data/silver/taxi_trips/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW dim_date AS
SELECT *
FROM read_parquet(
    'data/gold/dim_date/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW dim_zone AS
SELECT *
FROM read_parquet(
    'data/gold/dim_zone/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW dim_weather AS
SELECT *
FROM read_parquet(
    'data/gold/dim_weather/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW fact_taxi_trips AS
SELECT *
FROM read_parquet(
    'data/gold/fact_taxi_trips/**/*.parquet',
    hive_partitioning = true
);


-- 1) OVERVIEW
SELECT
    'dim_date' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(date_day) AS min_date,
    MAX(date_day) AS max_date
FROM dim_date

UNION ALL

SELECT
    'dim_weather' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(weather_date) AS min_date,
    MAX(weather_date) AS max_date
FROM dim_weather

UNION ALL

SELECT
    'fact_taxi_trips' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM fact_taxi_trips

UNION ALL

SELECT
    'dim_zone' AS dataset_name,
    COUNT(*) AS row_count,
    NULL AS min_date,
    NULL AS max_date
FROM dim_zone

ORDER BY dataset_name;


-- 2) FACT COVERAGE BY MONTH
SELECT
    pickup_year,
    pickup_month,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue
FROM fact_taxi_trips
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;


-- 3) OUT-OF-SCOPE SUMMARY
SELECT
    SUM(CASE WHEN date_day < DATE '2023-01-01' OR date_day > DATE '2023-03-31' THEN 1 ELSE 0 END) AS out_of_scope_dim_date_rows
FROM dim_date;

SELECT
    SUM(CASE WHEN weather_date < DATE '2023-01-01' OR weather_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS out_of_scope_dim_weather_rows
FROM dim_weather;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS out_of_scope_fact_pickup_date_rows,
    SUM(CASE WHEN weather_date < DATE '2023-01-01' OR weather_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS out_of_scope_fact_weather_date_rows,
    SUM(CASE WHEN pickup_year <> 2023 OR pickup_month NOT IN (1, 2, 3) THEN 1 ELSE 0 END) AS out_of_scope_fact_partition_rows
FROM fact_taxi_trips;


-- 4) DIM QUALITY CHECKS
-- 4.1) dim_date
SELECT
    SUM(CASE WHEN date_day IS NULL THEN 1 ELSE 0 END) AS date_day_nulls,
    SUM(CASE WHEN year_num IS NULL THEN 1 ELSE 0 END) AS year_num_nulls,
    SUM(CASE WHEN quarter_num IS NULL THEN 1 ELSE 0 END) AS quarter_num_nulls,
    SUM(CASE WHEN month_num IS NULL THEN 1 ELSE 0 END) AS month_num_nulls,
    SUM(CASE WHEN month_name IS NULL THEN 1 ELSE 0 END) AS month_name_nulls,
    SUM(CASE WHEN day_num_in_month IS NULL THEN 1 ELSE 0 END) AS day_num_nulls,
    SUM(CASE WHEN day_of_week_num IS NULL THEN 1 ELSE 0 END) AS day_of_week_num_nulls,
    SUM(CASE WHEN day_of_week_name IS NULL THEN 1 ELSE 0 END) AS day_of_week_name_nulls,
    SUM(CASE WHEN is_weekend IS NULL THEN 1 ELSE 0 END) AS is_weekend_nulls,
    SUM(CASE WHEN gold_loaded_at IS NULL THEN 1 ELSE 0 END) AS gold_loaded_at_nulls,
    SUM(CASE WHEN year_num <> YEAR(date_day) THEN 1 ELSE 0 END) AS bad_year_num_rows,
    SUM(CASE WHEN quarter_num <> QUARTER(date_day) THEN 1 ELSE 0 END) AS bad_quarter_num_rows,
    SUM(CASE WHEN month_num <> MONTH(date_day) THEN 1 ELSE 0 END) AS bad_month_num_rows,
    SUM(CASE WHEN day_num_in_month <> DAY(date_day) THEN 1 ELSE 0 END) AS bad_day_num_rows
FROM dim_date;

-- 4.2) dim_zone
SELECT
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS location_id_nulls,
    SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS borough_nulls,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS zone_nulls,
    SUM(CASE WHEN service_zone IS NULL THEN 1 ELSE 0 END) AS service_zone_nulls,
    SUM(CASE WHEN gold_loaded_at IS NULL THEN 1 ELSE 0 END) AS gold_loaded_at_nulls,
    SUM(CASE WHEN TRIM(COALESCE(borough, '')) = '' THEN 1 ELSE 0 END) AS blank_borough_rows,
    SUM(CASE WHEN TRIM(COALESCE(zone, '')) = '' THEN 1 ELSE 0 END) AS blank_zone_rows,
    SUM(CASE WHEN TRIM(COALESCE(service_zone, '')) = '' THEN 1 ELSE 0 END) AS blank_service_zone_rows
FROM dim_zone;

-- 4.3) dim_weather
SELECT
    SUM(CASE WHEN weather_date IS NULL THEN 1 ELSE 0 END) AS weather_date_nulls,
    SUM(CASE WHEN temperature_max IS NULL THEN 1 ELSE 0 END) AS temperature_max_nulls,
    SUM(CASE WHEN temperature_min IS NULL THEN 1 ELSE 0 END) AS temperature_min_nulls,
    SUM(CASE WHEN temperature_mean IS NULL THEN 1 ELSE 0 END) AS temperature_mean_nulls,
    SUM(CASE WHEN precipitation_sum IS NULL THEN 1 ELSE 0 END) AS precipitation_sum_nulls,
    SUM(CASE WHEN snowfall_sum IS NULL THEN 1 ELSE 0 END) AS snowfall_sum_nulls,
    SUM(CASE WHEN is_rainy_day IS NULL THEN 1 ELSE 0 END) AS is_rainy_day_nulls,
    SUM(CASE WHEN is_snowy_day IS NULL THEN 1 ELSE 0 END) AS is_snowy_day_nulls,
    SUM(CASE WHEN gold_loaded_at IS NULL THEN 1 ELSE 0 END) AS gold_loaded_at_nulls,
    SUM(CASE WHEN precipitation_sum > 0 AND is_rainy_day = FALSE THEN 1 ELSE 0 END) AS bad_rain_flag_rows,
    SUM(CASE WHEN precipitation_sum <= 0 AND is_rainy_day = TRUE THEN 1 ELSE 0 END) AS bad_non_rain_flag_rows,
    SUM(CASE WHEN snowfall_sum > 0 AND is_snowy_day = FALSE THEN 1 ELSE 0 END) AS bad_snow_flag_rows,
    SUM(CASE WHEN snowfall_sum <= 0 AND is_snowy_day = TRUE THEN 1 ELSE 0 END) AS bad_non_snow_flag_rows
FROM dim_weather;


-- 5) FACT QUALITY CHECKS
SELECT
    SUM(CASE WHEN trip_id IS NULL THEN 1 ELSE 0 END) AS trip_id_nulls,
    SUM(CASE WHEN pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_datetime_nulls,
    SUM(CASE WHEN dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS dropoff_datetime_nulls,
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN pickup_hour IS NULL THEN 1 ELSE 0 END) AS pickup_hour_nulls,
    SUM(CASE WHEN weather_date IS NULL THEN 1 ELSE 0 END) AS weather_date_nulls,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END) AS pickup_location_id_nulls,
    SUM(CASE WHEN dropoff_location_id IS NULL THEN 1 ELSE 0 END) AS dropoff_location_id_nulls,
    SUM(CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END) AS trip_distance_nulls,
    SUM(CASE WHEN trip_duration_minutes IS NULL THEN 1 ELSE 0 END) AS trip_duration_minutes_nulls,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS total_amount_nulls,
    SUM(CASE WHEN gold_loaded_at IS NULL THEN 1 ELSE 0 END) AS gold_loaded_at_nulls,

    SUM(CASE WHEN pickup_datetime >= dropoff_datetime THEN 1 ELSE 0 END) AS bad_time_order_rows,
    SUM(CASE WHEN trip_duration_minutes <= 0 THEN 1 ELSE 0 END) AS non_positive_duration_rows,
    SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_trip_distance_rows,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN pickup_hour <> HOUR(pickup_datetime) THEN 1 ELSE 0 END) AS bad_pickup_hour_rows,
    SUM(CASE WHEN pickup_date <> weather_date THEN 1 ELSE 0 END) AS bad_weather_date_match_rows,

    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare_amount_rows,
    SUM(CASE WHEN extra_amount < 0 THEN 1 ELSE 0 END) AS negative_extra_amount_rows,
    SUM(CASE WHEN mta_tax_amount < 0 THEN 1 ELSE 0 END) AS negative_mta_tax_amount_rows,
    SUM(CASE WHEN tip_amount < 0 THEN 1 ELSE 0 END) AS negative_tip_amount_rows,
    SUM(CASE WHEN tolls_amount < 0 THEN 1 ELSE 0 END) AS negative_tolls_amount_rows,
    SUM(CASE WHEN improvement_surcharge_amount < 0 THEN 1 ELSE 0 END) AS negative_improvement_surcharge_rows,
    SUM(CASE WHEN congestion_surcharge_amount < 0 THEN 1 ELSE 0 END) AS negative_congestion_surcharge_rows,
    SUM(CASE WHEN airport_fee_amount < 0 THEN 1 ELSE 0 END) AS negative_airport_fee_rows,
    SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total_amount_rows
FROM fact_taxi_trips;


-- 6) DUPLICATE CHECKS
WITH dim_date_dup AS (
    SELECT COUNT(*) AS duplicate_date_day_rows
    FROM (
        SELECT date_day
        FROM dim_date
        GROUP BY date_day
        HAVING COUNT(*) > 1
    )
),
dim_zone_dup AS (
    SELECT COUNT(*) AS duplicate_location_id_rows
    FROM (
        SELECT location_id
        FROM dim_zone
        GROUP BY location_id
        HAVING COUNT(*) > 1
    )
),
dim_weather_dup AS (
    SELECT COUNT(*) AS duplicate_weather_date_rows
    FROM (
        SELECT weather_date
        FROM dim_weather
        GROUP BY weather_date
        HAVING COUNT(*) > 1
    )
),
fact_dup AS (
    SELECT COUNT(*) AS duplicate_trip_id_rows
    FROM (
        SELECT trip_id
        FROM fact_taxi_trips
        GROUP BY trip_id
        HAVING COUNT(*) > 1
    )
)
SELECT
    duplicate_date_day_rows,
    duplicate_location_id_rows,
    duplicate_weather_date_rows,
    duplicate_trip_id_rows
FROM dim_date_dup, dim_zone_dup, dim_weather_dup, fact_dup;


-- 7) FACT FOREIGN KEY COVERAGE
SELECT
    SUM(CASE WHEN d.date_day IS NULL THEN 1 ELSE 0 END) AS missing_dim_date_rows,
    SUM(CASE WHEN p.location_id IS NULL THEN 1 ELSE 0 END) AS missing_pickup_dim_zone_rows,
    SUM(CASE WHEN z.location_id IS NULL THEN 1 ELSE 0 END) AS missing_dropoff_dim_zone_rows,
    SUM(CASE WHEN w.weather_date IS NULL THEN 1 ELSE 0 END) AS missing_dim_weather_rows
FROM fact_taxi_trips AS f
LEFT JOIN dim_date AS d
    ON f.pickup_date = d.date_day
LEFT JOIN dim_zone AS p
    ON f.pickup_location_id = p.location_id
LEFT JOIN dim_zone AS z
    ON f.dropoff_location_id = z.location_id
LEFT JOIN dim_weather AS w
    ON f.weather_date = w.weather_date;


-- 8) FACT VS SILVER RECONCILIATION
SELECT
    (SELECT COUNT(*) FROM silver_taxi_trips) AS silver_taxi_row_count,
    (SELECT COUNT(*) FROM fact_taxi_trips) AS fact_taxi_row_count,
    ROUND((SELECT SUM(total_amount) FROM silver_taxi_trips), 2) AS silver_total_amount_sum,
    ROUND((SELECT SUM(total_amount) FROM fact_taxi_trips), 2) AS fact_total_amount_sum;

SELECT
    COUNT(*) AS trip_ids_missing_in_fact
FROM (
    SELECT trip_id
    FROM silver_taxi_trips
    EXCEPT
    SELECT trip_id
    FROM fact_taxi_trips
);

SELECT
    COUNT(*) AS unexpected_trip_ids_in_fact
FROM (
    SELECT trip_id
    FROM fact_taxi_trips
    EXCEPT
    SELECT trip_id
    FROM silver_taxi_trips
);


-- 9) BUSINESS SANITY SNAPSHOTS
-- 9.1) Daily demand preview
SELECT
    pickup_date,
    COUNT(*) AS trip_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
    ROUND(AVG(trip_duration_minutes), 2) AS avg_trip_duration_minutes
FROM fact_taxi_trips
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 31;

-- 9.2) Top pickup zones
SELECT
    f.pickup_location_id,
    z.borough,
    z.zone,
    COUNT(*) AS trip_count,
    ROUND(SUM(f.total_amount), 2) AS total_revenue
FROM fact_taxi_trips AS f
LEFT JOIN dim_zone AS z
    ON f.pickup_location_id = z.location_id
GROUP BY
    f.pickup_location_id,
    z.borough,
    z.zone
ORDER BY trip_count DESC, total_revenue DESC
LIMIT 20;

-- 9.3) Negative amount profile
SELECT
    pickup_date,
    COUNT(*) AS negative_total_amount_trip_count,
    ROUND(SUM(total_amount), 2) AS negative_total_amount_sum
FROM fact_taxi_trips
WHERE total_amount < 0
GROUP BY pickup_date
ORDER BY pickup_date
LIMIT 31;