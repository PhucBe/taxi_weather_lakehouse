-- 0) RAW TABLE ROW COUNTS
SELECT 'raw_taxi_trips' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_taxi_trips

UNION ALL

SELECT 'raw_weather_daily' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_weather_daily

UNION ALL

SELECT 'raw_zone_lookup' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_zone_lookup;


-- 1) TAXI RAW QUALITY CHECK
WITH params AS (
    SELECT
        DATE '2023-01-01' AS expected_start_date,
        DATE '2023-03-31' AS expected_end_date
)
SELECT
    COUNT(*) AS taxi_row_count,

    MIN(CAST(tpep_pickup_datetime AS DATE)) AS min_pickup_date,
    MAX(CAST(tpep_pickup_datetime AS DATE)) AS max_pickup_date,

    SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_datetime_nulls,
    SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS dropoff_datetime_nulls,
    SUM(CASE WHEN pulocationid IS NULL THEN 1 ELSE 0 END) AS pulocationid_nulls,
    SUM(CASE WHEN dolocationid IS NULL THEN 1 ELSE 0 END) AS dolocationid_nulls,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS total_amount_nulls,

    SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_trip_distance_rows,
    SUM(CASE WHEN passenger_count < 0 THEN 1 ELSE 0 END) AS negative_passenger_count_rows,
    SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total_amount_rows,
    SUM(CASE WHEN tpep_dropoff_datetime < tpep_pickup_datetime THEN 1 ELSE 0 END) AS invalid_datetime_order_rows,

    SUM(
        CASE
            WHEN CAST(tpep_pickup_datetime AS DATE) < p.expected_start_date
              OR CAST(tpep_pickup_datetime AS DATE) > p.expected_end_date
            THEN 1
            ELSE 0
        END
    ) AS pickup_date_out_of_expected_range_rows

FROM raw_layer.raw_taxi_trips t
CROSS JOIN params p;


-- 2) WEATHER RAW QUALITY CHECK
SELECT
    COUNT(*) AS weather_row_count,
    COUNT(DISTINCT date) AS distinct_weather_dates,

    MIN(date) AS min_weather_date,
    MAX(date) AS max_weather_date,

    DATEDIFF(day, MIN(date), MAX(date)) + 1 AS expected_calendar_days,
    (DATEDIFF(day, MIN(date), MAX(date)) + 1) - COUNT(DISTINCT date) AS missing_weather_days,

    COUNT(*) - COUNT(DISTINCT date) AS duplicate_weather_date_rows,

    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS date_nulls,
    SUM(CASE WHEN temperature_2m_mean IS NULL THEN 1 ELSE 0 END) AS temperature_mean_nulls,
    SUM(CASE WHEN precipitation_sum IS NULL THEN 1 ELSE 0 END) AS precipitation_sum_nulls,
    SUM(CASE WHEN snowfall_sum IS NULL THEN 1 ELSE 0 END) AS snowfall_sum_nulls

FROM raw_layer.raw_weather_daily;


-- 3) ZONE LOOKUP QUALITY CHECK
SELECT
    COUNT(*) AS zone_row_count,
    COUNT(DISTINCT location_id) AS distinct_location_ids,
    COUNT(*) - COUNT(DISTINCT location_id) AS duplicate_location_id_rows,

    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS location_id_nulls,
    SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS borough_nulls,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS zone_nulls,
    SUM(CASE WHEN service_zone IS NULL THEN 1 ELSE 0 END) AS service_zone_nulls

FROM raw_layer.raw_zone_lookup;


-- 4) TAXI LOCATION MAPPING CHECK
SELECT
    'pickup_location_unmapped' AS check_name,
    COUNT(*) AS unmapped_rows
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.pulocationid = z.location_id
WHERE z.location_id IS NULL

UNION ALL

SELECT
    'dropoff_location_unmapped' AS check_name,
    COUNT(*) AS unmapped_rows
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.dolocationid = z.location_id
WHERE z.location_id IS NULL;


-- 5) TAXI DAILY COVERAGE VS WEATHER
WITH taxi_daily AS (
    SELECT
        CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue
    FROM raw_layer.raw_taxi_trips
    GROUP BY CAST(tpep_pickup_datetime AS DATE)
)
SELECT
    COUNT(*) AS taxi_active_days,
    SUM(CASE WHEN w.date IS NULL THEN 1 ELSE 0 END) AS taxi_days_missing_weather,
    MIN(t.pickup_date) AS min_taxi_daily_date,
    MAX(t.pickup_date) AS max_taxi_daily_date
FROM taxi_daily t
LEFT JOIN raw_layer.raw_weather_daily w
    ON t.pickup_date = w.date;


-- 6) PAYMENT TYPE DISTRIBUTION
SELECT
    payment_type,
    COUNT(*) AS trip_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS trip_share_pct
FROM raw_layer.raw_taxi_trips
GROUP BY payment_type
ORDER BY payment_type;


-- 7) RAW BUSINESS METRICS QUICK CHECK
SELECT
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue_raw,
    AVG(total_amount) AS avg_total_amount_raw,
    AVG(trip_distance) AS avg_trip_distance_raw
FROM raw_layer.raw_taxi_trips;


-- 8) FINAL PASS / FAIL SUMMARY
WITH
taxi AS (
    SELECT
        COUNT(*) AS row_count,
        SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_nulls,
        SUM(CASE WHEN pulocationid IS NULL THEN 1 ELSE 0 END) AS pickup_location_nulls,
        SUM(CASE WHEN dolocationid IS NULL THEN 1 ELSE 0 END) AS dropoff_location_nulls
    FROM raw_layer.raw_taxi_trips
),

weather AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(DISTINCT date) AS distinct_dates,
        (DATEDIFF(day, MIN(date), MAX(date)) + 1) - COUNT(DISTINCT date) AS missing_days
    FROM raw_layer.raw_weather_daily
),

zone AS (
    SELECT
        COUNT(*) AS row_count,
        COUNT(*) - COUNT(DISTINCT location_id) AS duplicate_location_ids
    FROM raw_layer.raw_zone_lookup
)

SELECT
    CASE
        WHEN taxi.row_count > 0
         AND weather.row_count > 0
         AND zone.row_count > 0
         AND taxi.pickup_nulls = 0
         AND taxi.pickup_location_nulls = 0
         AND taxi.dropoff_location_nulls = 0
         AND weather.missing_days = 0
         AND zone.duplicate_location_ids = 0
        THEN 'PASS'
        ELSE 'CHECK_REQUIRED'
    END AS raw_layer_status,

    taxi.row_count AS taxi_rows,
    weather.row_count AS weather_rows,
    zone.row_count AS zone_rows,

    taxi.pickup_nulls,
    taxi.pickup_location_nulls,
    taxi.dropoff_location_nulls,
    weather.missing_days,
    zone.duplicate_location_ids

FROM taxi
CROSS JOIN weather
CROSS JOIN zone;