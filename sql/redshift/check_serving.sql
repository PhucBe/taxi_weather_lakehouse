-- 1) OVERVIEW
SELECT
    'mart_daily_demand' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM serving.mart_daily_demand

UNION ALL

SELECT
    'mart_daily_payment_mix' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM serving.mart_daily_payment_mix

UNION ALL

SELECT
    'mart_weather_impact' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM serving.mart_weather_impact

UNION ALL

SELECT
    'mart_zone_demand' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM serving.mart_zone_demand

ORDER BY dataset_name;


-- 2) MONTH COVERAGE
SELECT
    'mart_daily_demand' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM serving.mart_daily_demand
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_daily_payment_mix' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM serving.mart_daily_payment_mix
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_weather_impact' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM serving.mart_weather_impact
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_zone_demand' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM serving.mart_zone_demand
GROUP BY pickup_year, pickup_month

ORDER BY dataset_name, pickup_year, pickup_month;


-- 3) OUT-OF-SCOPE SUMMARY
SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS daily_demand_out_of_scope_rows
FROM serving.mart_daily_demand;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS payment_mix_out_of_scope_rows
FROM serving.mart_daily_payment_mix;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS weather_impact_pickup_out_of_scope_rows,
    SUM(CASE WHEN weather_date < DATE '2023-01-01' OR weather_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS weather_impact_weather_out_of_scope_rows
FROM serving.mart_weather_impact;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS zone_demand_out_of_scope_rows
FROM serving.mart_zone_demand;


-- 4) CORE NULL / QUALITY CHECKS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN pickup_year <> EXTRACT(YEAR FROM pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> EXTRACT(MONTH FROM pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows
FROM serving.mart_daily_demand;

SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN payment_type_code IS NULL THEN 1 ELSE 0 END) AS payment_type_code_nulls,
    SUM(CASE WHEN payment_type_name IS NULL THEN 1 ELSE 0 END) AS payment_type_name_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN payment_trip_share_pct IS NULL THEN 1 ELSE 0 END) AS payment_trip_share_pct_nulls,
    SUM(CASE WHEN payment_trip_share_pct < 0 OR payment_trip_share_pct > 1 THEN 1 ELSE 0 END) AS out_of_range_trip_share_rows
FROM serving.mart_daily_payment_mix;

SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN weather_date IS NULL THEN 1 ELSE 0 END) AS weather_date_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN pickup_date <> weather_date THEN 1 ELSE 0 END) AS bad_weather_match_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows
FROM serving.mart_weather_impact;

SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END) AS pickup_location_id_nulls,
    SUM(CASE WHEN borough IS NULL OR TRIM(COALESCE(borough, '')) = '' THEN 1 ELSE 0 END) AS bad_borough_rows,
    SUM(CASE WHEN zone IS NULL OR TRIM(COALESCE(zone, '')) = '' THEN 1 ELSE 0 END) AS bad_zone_rows,
    SUM(CASE WHEN service_zone IS NULL OR TRIM(COALESCE(service_zone, '')) = '' THEN 1 ELSE 0 END) AS bad_service_zone_rows,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows
FROM serving.mart_zone_demand;


-- 5) DUPLICATE GRAIN CHECKS
WITH daily_demand_dup AS (
    SELECT COUNT(*) AS duplicate_pickup_date_rows
    FROM (
        SELECT pickup_date
        FROM serving.mart_daily_demand
        GROUP BY pickup_date
        HAVING COUNT(*) > 1
    ) t
),
payment_mix_dup AS (
    SELECT COUNT(*) AS duplicate_payment_mix_rows
    FROM (
        SELECT pickup_date, payment_type_code
        FROM serving.mart_daily_payment_mix
        GROUP BY pickup_date, payment_type_code
        HAVING COUNT(*) > 1
    ) t
),
weather_impact_dup AS (
    SELECT COUNT(*) AS duplicate_weather_impact_rows
    FROM (
        SELECT pickup_date
        FROM serving.mart_weather_impact
        GROUP BY pickup_date
        HAVING COUNT(*) > 1
    ) t
),
zone_demand_dup AS (
    SELECT COUNT(*) AS duplicate_zone_demand_rows
    FROM (
        SELECT pickup_date, pickup_location_id
        FROM serving.mart_zone_demand
        GROUP BY pickup_date, pickup_location_id
        HAVING COUNT(*) > 1
    ) t
)
SELECT
    duplicate_pickup_date_rows,
    duplicate_payment_mix_rows,
    duplicate_weather_impact_rows,
    duplicate_zone_demand_rows
FROM daily_demand_dup, payment_mix_dup, weather_impact_dup, zone_demand_dup;


-- 6) CROSS-MART RECONCILIATION
-- 6.1) daily_demand vs weather_impact
SELECT
    COUNT(*) AS daily_vs_weather_mismatch_rows
FROM serving.mart_daily_demand d
FULL OUTER JOIN serving.mart_weather_impact w
    ON d.pickup_date = w.pickup_date
WHERE d.pickup_date IS NULL
   OR w.pickup_date IS NULL
   OR d.trip_count <> w.trip_count
   OR ABS(d.total_revenue - w.total_revenue) > 0.01
   OR d.negative_total_amount_trip_count <> w.negative_total_amount_trip_count;

-- 6.2) daily_demand vs payment_mix rolled up by date
WITH payment_mix_daily AS (
    SELECT
        pickup_date,
        SUM(trip_count) AS trip_count_sum,
        SUM(total_revenue) AS total_revenue_sum
    FROM serving.mart_daily_payment_mix
    GROUP BY pickup_date
)
SELECT
    COUNT(*) AS daily_vs_payment_mix_mismatch_rows
FROM serving.mart_daily_demand d
FULL OUTER JOIN payment_mix_daily p
    ON d.pickup_date = p.pickup_date
WHERE d.pickup_date IS NULL
   OR p.pickup_date IS NULL
   OR d.trip_count <> p.trip_count_sum
   OR ABS(d.total_revenue - p.total_revenue_sum) > 0.01;

-- 6.3) daily_demand vs zone_demand rolled up by date
WITH zone_daily AS (
    SELECT
        pickup_date,
        SUM(trip_count) AS trip_count_sum,
        SUM(total_revenue) AS total_revenue_sum,
        SUM(negative_total_amount_trip_count) AS negative_trip_count_sum
    FROM serving.mart_zone_demand
    GROUP BY pickup_date
)
SELECT
    COUNT(*) AS daily_vs_zone_mismatch_rows
FROM serving.mart_daily_demand d
FULL OUTER JOIN zone_daily z
    ON d.pickup_date = z.pickup_date
WHERE d.pickup_date IS NULL
   OR z.pickup_date IS NULL
   OR d.trip_count <> z.trip_count_sum
   OR ABS(d.total_revenue - z.total_revenue_sum) > 0.01
   OR d.negative_total_amount_trip_count <> z.negative_trip_count_sum;


-- 7) PAYMENT SHARE CHECKS
SELECT
    pickup_date,
    ROUND(SUM(payment_trip_share_pct), 6) AS trip_share_sum
FROM serving.mart_daily_payment_mix
GROUP BY pickup_date
HAVING ABS(SUM(payment_trip_share_pct) - 1.0) > 0.0001
ORDER BY pickup_date;

SELECT
    pickup_date,
    ROUND(SUM(payment_revenue_share_pct), 6) AS revenue_share_sum,
    ROUND(SUM(total_revenue), 2) AS daily_revenue_sum
FROM serving.mart_daily_payment_mix
GROUP BY pickup_date
HAVING ABS(SUM(total_revenue)) > 0.01
   AND ABS(SUM(payment_revenue_share_pct) - 1.0) > 0.0001
ORDER BY pickup_date;


-- 8) SNAPSHOT FOR QUICK REVIEW
SELECT
    pickup_date,
    trip_count,
    ROUND(total_revenue, 2) AS total_revenue,
    negative_total_amount_trip_count
FROM serving.mart_daily_demand
ORDER BY pickup_date
LIMIT 15;

SELECT
    pickup_date,
    payment_type_code,
    payment_type_name,
    trip_count,
    ROUND(total_revenue, 2) AS total_revenue,
    ROUND(payment_trip_share_pct, 6) AS payment_trip_share_pct
FROM serving.mart_daily_payment_mix
ORDER BY pickup_date, trip_count DESC, payment_type_code
LIMIT 20;