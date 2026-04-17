-- =========================================================
-- CHECK_RAW.SQL
-- Project: Taxi Weather Lakehouse
-- Layer: raw_layer
-- Mục tiêu:
-- 1) Kiểm tra row count
-- 2) Kiểm tra date range
-- 3) Kiểm tra null / invalid values
-- 4) Kiểm tra consistency giữa taxi và zone
-- 5) Preview dữ liệu raw
-- =========================================================


-- =========================================================
-- 0) QUICK PREVIEW
-- =========================================================
SELECT *
FROM raw_layer.raw_taxi_trips
LIMIT 20;

SELECT *
FROM raw_layer.raw_weather_daily
LIMIT 20;

SELECT *
FROM raw_layer.raw_zone_lookup
LIMIT 20;


-- =========================================================
-- 1) ROW COUNT TOÀN BỘ RAW TABLES
-- =========================================================
SELECT 'raw_taxi_trips' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_taxi_trips

UNION ALL

SELECT 'raw_weather_daily' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_weather_daily

UNION ALL

SELECT 'raw_zone_lookup' AS table_name, COUNT(*) AS row_count
FROM raw_layer.raw_zone_lookup;


-- =========================================================
-- 2) DATE RANGE / BASIC COVERAGE
-- =========================================================
SELECT
    MIN(CAST(tpep_pickup_datetime AS DATE)) AS min_pickup_date,
    MAX(CAST(tpep_pickup_datetime AS DATE)) AS max_pickup_date,
    MIN(CAST(tpep_dropoff_datetime AS DATE)) AS min_dropoff_date,
    MAX(CAST(tpep_dropoff_datetime AS DATE)) AS max_dropoff_date
FROM raw_layer.raw_taxi_trips;

SELECT
    MIN(date) AS min_weather_date,
    MAX(date) AS max_weather_date,
    COUNT(*) AS weather_row_count
FROM raw_layer.raw_weather_daily;

SELECT
    COUNT(*) AS zone_row_count,
    COUNT(DISTINCT location_id) AS distinct_location_id_count
FROM raw_layer.raw_zone_lookup;


-- =========================================================
-- 3) TAXI RAW - NULL CHECKS Ở CỘT QUAN TRỌNG
-- =========================================================
SELECT
    SUM(CASE WHEN vendorid IS NULL THEN 1 ELSE 0 END) AS vendorid_nulls,
    SUM(CASE WHEN tpep_pickup_datetime IS NULL THEN 1 ELSE 0 END) AS pickup_datetime_nulls,
    SUM(CASE WHEN tpep_dropoff_datetime IS NULL THEN 1 ELSE 0 END) AS dropoff_datetime_nulls,
    SUM(CASE WHEN pulocationid IS NULL THEN 1 ELSE 0 END) AS pulocationid_nulls,
    SUM(CASE WHEN dolocationid IS NULL THEN 1 ELSE 0 END) AS dolocationid_nulls,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS payment_type_nulls,
    SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) AS total_amount_nulls
FROM raw_layer.raw_taxi_trips;


-- =========================================================
-- 4) TAXI RAW - KIỂM TRA GIÁ TRỊ BẤT THƯỜNG
-- =========================================================
SELECT
    SUM(CASE WHEN trip_distance < 0 THEN 1 ELSE 0 END) AS negative_trip_distance_rows,
    SUM(CASE WHEN fare_amount < 0 THEN 1 ELSE 0 END) AS negative_fare_amount_rows,
    SUM(CASE WHEN total_amount < 0 THEN 1 ELSE 0 END) AS negative_total_amount_rows,
    SUM(CASE WHEN passenger_count < 0 THEN 1 ELSE 0 END) AS negative_passenger_count_rows,
    SUM(CASE WHEN tpep_dropoff_datetime < tpep_pickup_datetime THEN 1 ELSE 0 END) AS invalid_datetime_order_rows
FROM raw_layer.raw_taxi_trips;


-- =========================================================
-- 5) TAXI RAW - PAYMENT TYPE DISTRIBUTION
-- =========================================================
SELECT
    payment_type,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips
GROUP BY payment_type
ORDER BY payment_type;


-- =========================================================
-- 6) TAXI RAW - PICKUP / DROPOFF DATE DISTRIBUTION
-- =========================================================
SELECT
    CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips
GROUP BY CAST(tpep_pickup_datetime AS DATE)
ORDER BY pickup_date;

SELECT
    CAST(tpep_dropoff_datetime AS DATE) AS dropoff_date,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips
GROUP BY CAST(tpep_dropoff_datetime AS DATE)
ORDER BY dropoff_date;


-- =========================================================
-- 7) WEATHER RAW - NULL CHECKS
-- =========================================================
SELECT
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS date_nulls,
    SUM(CASE WHEN temperature_2m_max IS NULL THEN 1 ELSE 0 END) AS temperature_2m_max_nulls,
    SUM(CASE WHEN temperature_2m_min IS NULL THEN 1 ELSE 0 END) AS temperature_2m_min_nulls,
    SUM(CASE WHEN temperature_2m_mean IS NULL THEN 1 ELSE 0 END) AS temperature_2m_mean_nulls,
    SUM(CASE WHEN precipitation_sum IS NULL THEN 1 ELSE 0 END) AS precipitation_sum_nulls,
    SUM(CASE WHEN snowfall_sum IS NULL THEN 1 ELSE 0 END) AS snowfall_sum_nulls
FROM raw_layer.raw_weather_daily;


-- =========================================================
-- 8) WEATHER RAW - DUPLICATE DATE CHECK
-- =========================================================
SELECT
    date,
    COUNT(*) AS row_count
FROM raw_layer.raw_weather_daily
GROUP BY date
HAVING COUNT(*) > 1
ORDER BY date;


-- =========================================================
-- 9) WEATHER RAW - THIẾU NGÀY HAY KHÔNG
-- =========================================================
WITH RECURSIVE date_spine(dt) AS (
    SELECT MIN(date)::DATE AS dt
    FROM raw_layer.raw_weather_daily

    UNION ALL

    SELECT DATEADD(DAY, 1, dt)::DATE
    FROM date_spine
    WHERE dt < (
        SELECT MAX(date)::DATE
        FROM raw_layer.raw_weather_daily
    )
)
SELECT
    ds.dt AS missing_date
FROM date_spine ds
LEFT JOIN raw_layer.raw_weather_daily w
    ON w.date = ds.dt
WHERE w.date IS NULL
ORDER BY ds.dt;


-- =========================================================
-- 10) ZONE RAW - NULL CHECKS
-- =========================================================
SELECT
    SUM(CASE WHEN location_id IS NULL THEN 1 ELSE 0 END) AS location_id_nulls,
    SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS borough_nulls,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS zone_nulls,
    SUM(CASE WHEN service_zone IS NULL THEN 1 ELSE 0 END) AS service_zone_nulls
FROM raw_layer.raw_zone_lookup;


-- =========================================================
-- 11) ZONE RAW - DUPLICATE LOCATION_ID CHECK
-- =========================================================
SELECT
    location_id,
    COUNT(*) AS row_count
FROM raw_layer.raw_zone_lookup
GROUP BY location_id
HAVING COUNT(*) > 1
ORDER BY location_id;


-- =========================================================
-- 12) TAXI VS ZONE - UNMAPPED PICKUP LOCATION
-- =========================================================
SELECT
    t.pulocationid,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.pulocationid = z.location_id
WHERE z.location_id IS NULL
GROUP BY t.pulocationid
ORDER BY trip_count DESC, t.pulocationid;


-- =========================================================
-- 13) TAXI VS ZONE - UNMAPPED DROPOFF LOCATION
-- =========================================================
SELECT
    t.dolocationid,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.dolocationid = z.location_id
WHERE z.location_id IS NULL
GROUP BY t.dolocationid
ORDER BY trip_count DESC, t.dolocationid;


-- =========================================================
-- 14) TOP PICKUP ZONES THEO SỐ CHUYẾN
-- =========================================================
SELECT
    z.borough,
    z.zone,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.pulocationid = z.location_id
GROUP BY z.borough, z.zone
ORDER BY trip_count DESC
LIMIT 20;


-- =========================================================
-- 15) TOP DROPOFF ZONES THEO SỐ CHUYẾN
-- =========================================================
SELECT
    z.borough,
    z.zone,
    COUNT(*) AS trip_count
FROM raw_layer.raw_taxi_trips t
LEFT JOIN raw_layer.raw_zone_lookup z
    ON t.dolocationid = z.location_id
GROUP BY z.borough, z.zone
ORDER BY trip_count DESC
LIMIT 20;


-- =========================================================
-- 16) TAXI RAW - CHECK APPROX REVENUE
-- =========================================================
SELECT
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_total_amount,
    AVG(trip_distance) AS avg_trip_distance
FROM raw_layer.raw_taxi_trips;


-- =========================================================
-- 17) WEATHER IMPACT QUICK CHECK Ở RAW LEVEL
-- =========================================================
WITH TAXI_DAILY AS (
    SELECT
        CAST(tpep_pickup_datetime AS DATE) AS trip_date,
        COUNT(*) AS trip_count,
        SUM(total_amount) AS total_revenue
    FROM raw_layer.raw_taxi_trips
    GROUP BY CAST(tpep_pickup_datetime AS DATE)
)
SELECT
    t.trip_date,
    t.trip_count,
    t.total_revenue,
    w.temperature_2m_mean,
    w.precipitation_sum,
    w.snowfall_sum
FROM TAXI_DAILY t
LEFT JOIN raw_layer.raw_weather_daily w
    ON t.trip_date = w.date
ORDER BY t.trip_date;


-- =========================================================
-- 18) QUICK PASS/FAIL SUMMARY
-- =========================================================
SELECT
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS taxi_has_data
FROM raw_layer.raw_taxi_trips;

SELECT
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS weather_has_data
FROM raw_layer.raw_weather_daily;

SELECT
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS zone_has_data
FROM raw_layer.raw_zone_lookup;


-- =========================================================
-- 19) Xem những dòng taxi sớm nhất
-- =========================================================
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    vendorid,
    pulocationid,
    dolocationid,
    trip_distance,
    fare_amount,
    total_amount
FROM raw_layer.raw_taxi_trips
ORDER BY tpep_pickup_datetime
LIMIT 50;


-- =========================================================
-- 20) Đếm các dòng nằm ngoài khoảng bạn kỳ vọng
-- =========================================================
SELECT
    COUNT(*) AS rows_before_2023_01_01
FROM raw_layer.raw_taxi_trips
WHERE CAST(tpep_pickup_datetime AS DATE) < DATE '2023-01-01';

SELECT
    COUNT(*) AS rows_after_2023_03_31
FROM raw_layer.raw_taxi_trips
WHERE CAST(tpep_pickup_datetime AS DATE) > DATE '2023-03-31';


-- =========================================================
-- 21) Nhìn phân bố theo năm để thấy outlier rõ hơn
-- =========================================================
SELECT
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,
    COUNT(*) AS row_count
FROM raw_layer.raw_taxi_trips
GROUP BY EXTRACT(YEAR FROM tpep_pickup_datetime)
ORDER BY pickup_year;


-- =========================================================
-- 22) Xem riêng các record “lỗi khoảng ngày”
-- =========================================================
SELECT
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    payment_type,
    pulocationid,
    dolocationid
FROM raw_layer.raw_taxi_trips
WHERE CAST(tpep_pickup_datetime AS DATE) < DATE '2023-01-01'
   OR CAST(tpep_pickup_datetime AS DATE) > DATE '2023-03-31'
ORDER BY tpep_pickup_datetime
LIMIT 100;