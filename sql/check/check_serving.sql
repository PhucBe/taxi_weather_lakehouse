-- 0) LOAD GOLD + SERVING DATA AS VIEWS
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

CREATE OR REPLACE VIEW mart_daily_demand AS
SELECT *
FROM read_parquet(
    'data/serving/mart_daily_demand/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW mart_daily_payment_mix AS
SELECT *
FROM read_parquet(
    'data/serving/mart_daily_payment_mix/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW mart_weather_impact AS
SELECT *
FROM read_parquet(
    'data/serving/mart_weather_impact/**/*.parquet',
    hive_partitioning = true
);

CREATE OR REPLACE VIEW mart_zone_demand AS
SELECT *
FROM read_parquet(
    'data/serving/mart_zone_demand/**/*.parquet',
    hive_partitioning = true
);


-- 1) OVERVIEW
SELECT
    'mart_daily_demand' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM mart_daily_demand

UNION ALL

SELECT
    'mart_daily_payment_mix' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM mart_daily_payment_mix

UNION ALL

SELECT
    'mart_weather_impact' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM mart_weather_impact

UNION ALL

SELECT
    'mart_zone_demand' AS dataset_name,
    COUNT(*) AS row_count,
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM mart_zone_demand

ORDER BY dataset_name;


-- 2) SERVING COVERAGE BY MONTH
SELECT
    'mart_daily_demand' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM mart_daily_demand
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_daily_payment_mix' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM mart_daily_payment_mix
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_weather_impact' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM mart_weather_impact
GROUP BY pickup_year, pickup_month

UNION ALL

SELECT
    'mart_zone_demand' AS dataset_name,
    pickup_year,
    pickup_month,
    COUNT(*) AS row_count
FROM mart_zone_demand
GROUP BY pickup_year, pickup_month

ORDER BY dataset_name, pickup_year, pickup_month;


-- 3) OUT-OF-SCOPE CHECKS
-- 3.1) SUMMARY
SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS daily_demand_out_of_scope_rows
FROM mart_daily_demand;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS payment_mix_out_of_scope_rows
FROM mart_daily_payment_mix;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS weather_impact_pickup_out_of_scope_rows,
    SUM(CASE WHEN weather_date < DATE '2023-01-01' OR weather_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS weather_impact_weather_out_of_scope_rows
FROM mart_weather_impact;

SELECT
    SUM(CASE WHEN pickup_date < DATE '2023-01-01' OR pickup_date > DATE '2023-03-31' THEN 1 ELSE 0 END) AS zone_demand_out_of_scope_rows
FROM mart_zone_demand;

-- 3.2) DETAILS
SELECT
    pickup_date,
    COUNT(*) AS row_count
FROM mart_daily_demand
WHERE pickup_date < DATE '2023-01-01'
   OR pickup_date > DATE '2023-03-31'
GROUP BY pickup_date
ORDER BY pickup_date;

SELECT
    pickup_date,
    COUNT(*) AS row_count
FROM mart_daily_payment_mix
WHERE pickup_date < DATE '2023-01-01'
   OR pickup_date > DATE '2023-03-31'
GROUP BY pickup_date
ORDER BY pickup_date;

SELECT
    pickup_date,
    weather_date,
    COUNT(*) AS row_count
FROM mart_weather_impact
WHERE pickup_date < DATE '2023-01-01'
   OR pickup_date > DATE '2023-03-31'
   OR weather_date < DATE '2023-01-01'
   OR weather_date > DATE '2023-03-31'
GROUP BY pickup_date, weather_date
ORDER BY pickup_date, weather_date;

SELECT
    pickup_date,
    COUNT(*) AS row_count
FROM mart_zone_demand
WHERE pickup_date < DATE '2023-01-01'
   OR pickup_date > DATE '2023-03-31'
GROUP BY pickup_date
ORDER BY pickup_date;


-- 4) MART_DAILY_DEMAND QUALITY CHECKS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN quarter_num IS NULL THEN 1 ELSE 0 END) AS quarter_num_nulls,
    SUM(CASE WHEN month_name IS NULL THEN 1 ELSE 0 END) AS month_name_nulls,
    SUM(CASE WHEN day_num_in_month IS NULL THEN 1 ELSE 0 END) AS day_num_in_month_nulls,
    SUM(CASE WHEN day_of_week_num IS NULL THEN 1 ELSE 0 END) AS day_of_week_num_nulls,
    SUM(CASE WHEN day_of_week_name IS NULL THEN 1 ELSE 0 END) AS day_of_week_name_nulls,
    SUM(CASE WHEN is_weekend IS NULL THEN 1 ELSE 0 END) AS is_weekend_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN avg_total_amount IS NULL THEN 1 ELSE 0 END) AS avg_total_amount_nulls,
    SUM(CASE WHEN negative_total_amount_trip_count IS NULL THEN 1 ELSE 0 END) AS negative_total_amount_trip_count_nulls,
    SUM(CASE WHEN serving_loaded_at IS NULL THEN 1 ELSE 0 END) AS serving_loaded_at_nulls,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows,
    SUM(CASE WHEN negative_total_amount_trip_count < 0 THEN 1 ELSE 0 END) AS negative_negative_trip_count_rows
FROM mart_daily_demand;


-- 5) MART_DAILY_PAYMENT_MIX QUALITY CHECKS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN payment_type_code IS NULL THEN 1 ELSE 0 END) AS payment_type_code_nulls,
    SUM(CASE WHEN payment_type_name IS NULL THEN 1 ELSE 0 END) AS payment_type_name_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN payment_trip_share_pct IS NULL THEN 1 ELSE 0 END) AS payment_trip_share_pct_nulls,
    SUM(CASE WHEN payment_revenue_share_pct IS NULL THEN 1 ELSE 0 END) AS payment_revenue_share_pct_nulls,
    SUM(CASE WHEN serving_loaded_at IS NULL THEN 1 ELSE 0 END) AS serving_loaded_at_nulls,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows,
    SUM(CASE WHEN payment_trip_share_pct < 0 OR payment_trip_share_pct > 1 THEN 1 ELSE 0 END) AS out_of_range_trip_share_rows,
    SUM(CASE WHEN payment_revenue_share_pct < 0 OR payment_revenue_share_pct > 1 THEN 1 ELSE 0 END) AS out_of_range_revenue_share_rows
FROM mart_daily_payment_mix;


-- 6) MART_WEATHER_IMPACT QUALITY CHECKS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN day_of_week_num IS NULL THEN 1 ELSE 0 END) AS day_of_week_num_nulls,
    SUM(CASE WHEN day_of_week_name IS NULL THEN 1 ELSE 0 END) AS day_of_week_name_nulls,
    SUM(CASE WHEN is_weekend IS NULL THEN 1 ELSE 0 END) AS is_weekend_nulls,
    SUM(CASE WHEN weather_date IS NULL THEN 1 ELSE 0 END) AS weather_date_nulls,
    SUM(CASE WHEN is_rainy_day IS NULL THEN 1 ELSE 0 END) AS is_rainy_day_nulls,
    SUM(CASE WHEN is_snowy_day IS NULL THEN 1 ELSE 0 END) AS is_snowy_day_nulls,
    SUM(CASE WHEN temperature_mean IS NULL THEN 1 ELSE 0 END) AS temperature_mean_nulls,
    SUM(CASE WHEN precipitation_sum IS NULL THEN 1 ELSE 0 END) AS precipitation_sum_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN negative_total_amount_trip_count IS NULL THEN 1 ELSE 0 END) AS negative_total_amount_trip_count_nulls,
    SUM(CASE WHEN serving_loaded_at IS NULL THEN 1 ELSE 0 END) AS serving_loaded_at_nulls,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN pickup_date <> weather_date THEN 1 ELSE 0 END) AS bad_weather_match_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows
FROM mart_weather_impact;


-- 7) MART_ZONE_DEMAND QUALITY CHECKS
SELECT
    SUM(CASE WHEN pickup_date IS NULL THEN 1 ELSE 0 END) AS pickup_date_nulls,
    SUM(CASE WHEN pickup_year IS NULL THEN 1 ELSE 0 END) AS pickup_year_nulls,
    SUM(CASE WHEN pickup_month IS NULL THEN 1 ELSE 0 END) AS pickup_month_nulls,
    SUM(CASE WHEN pickup_location_id IS NULL THEN 1 ELSE 0 END) AS pickup_location_id_nulls,
    SUM(CASE WHEN borough IS NULL THEN 1 ELSE 0 END) AS borough_nulls,
    SUM(CASE WHEN zone IS NULL THEN 1 ELSE 0 END) AS zone_nulls,
    SUM(CASE WHEN service_zone IS NULL THEN 1 ELSE 0 END) AS service_zone_nulls,
    SUM(CASE WHEN trip_count IS NULL THEN 1 ELSE 0 END) AS trip_count_nulls,
    SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) AS total_revenue_nulls,
    SUM(CASE WHEN negative_total_amount_trip_count IS NULL THEN 1 ELSE 0 END) AS negative_total_amount_trip_count_nulls,
    SUM(CASE WHEN serving_loaded_at IS NULL THEN 1 ELSE 0 END) AS serving_loaded_at_nulls,
    SUM(CASE WHEN pickup_year <> YEAR(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_year_rows,
    SUM(CASE WHEN pickup_month <> MONTH(pickup_date) THEN 1 ELSE 0 END) AS bad_pickup_month_rows,
    SUM(CASE WHEN TRIM(COALESCE(borough, '')) = '' THEN 1 ELSE 0 END) AS blank_borough_rows,
    SUM(CASE WHEN TRIM(COALESCE(zone, '')) = '' THEN 1 ELSE 0 END) AS blank_zone_rows,
    SUM(CASE WHEN TRIM(COALESCE(service_zone, '')) = '' THEN 1 ELSE 0 END) AS blank_service_zone_rows,
    SUM(CASE WHEN trip_count <= 0 THEN 1 ELSE 0 END) AS non_positive_trip_count_rows
FROM mart_zone_demand;


-- 8) UNIQUENESS CHECKS
WITH daily_demand_dup AS (
    SELECT COUNT(*) AS duplicate_pickup_date_rows
    FROM (
        SELECT pickup_date
        FROM mart_daily_demand
        GROUP BY pickup_date
        HAVING COUNT(*) > 1
    )
),
payment_mix_dup AS (
    SELECT COUNT(*) AS duplicate_payment_mix_rows
    FROM (
        SELECT pickup_date, payment_type_code
        FROM mart_daily_payment_mix
        GROUP BY pickup_date, payment_type_code
        HAVING COUNT(*) > 1
    )
),
weather_impact_dup AS (
    SELECT COUNT(*) AS duplicate_weather_impact_rows
    FROM (
        SELECT pickup_date
        FROM mart_weather_impact
        GROUP BY pickup_date
        HAVING COUNT(*) > 1
    )
),
zone_demand_dup AS (
    SELECT COUNT(*) AS duplicate_zone_demand_rows
    FROM (
        SELECT pickup_date, pickup_location_id
        FROM mart_zone_demand
        GROUP BY pickup_date, pickup_location_id
        HAVING COUNT(*) > 1
    )
)
SELECT
    daily_demand_dup.duplicate_pickup_date_rows,
    payment_mix_dup.duplicate_payment_mix_rows,
    weather_impact_dup.duplicate_weather_impact_rows,
    zone_demand_dup.duplicate_zone_demand_rows
FROM daily_demand_dup, payment_mix_dup, weather_impact_dup, zone_demand_dup;


-- 9) DATE / WEATHER / ZONE DESCRIPTOR CONSISTENCY
-- 9.1) mart_daily_demand vs dim_date
SELECT
    COUNT(*) AS mart_daily_demand_date_mismatch_rows
FROM mart_daily_demand AS m
LEFT JOIN dim_date AS d
    ON m.pickup_date = d.date_day
WHERE d.date_day IS NULL
   OR m.quarter_num <> d.quarter_num
   OR m.month_name <> d.month_name
   OR m.day_num_in_month <> d.day_num_in_month
   OR m.day_of_week_num <> d.day_of_week_num
   OR m.day_of_week_name <> d.day_of_week_name
   OR m.is_weekend <> d.is_weekend;

-- 9.2) mart_weather_impact vs dim_date + dim_weather
SELECT
    COUNT(*) AS mart_weather_impact_descriptor_mismatch_rows
FROM mart_weather_impact AS m
LEFT JOIN dim_date AS d
    ON m.pickup_date = d.date_day
LEFT JOIN dim_weather AS w
    ON m.weather_date = w.weather_date
WHERE d.date_day IS NULL
   OR w.weather_date IS NULL
   OR m.weather_date <> m.pickup_date
   OR m.day_of_week_num <> d.day_of_week_num
   OR m.day_of_week_name <> d.day_of_week_name
   OR m.is_weekend <> d.is_weekend
   OR m.is_rainy_day <> w.is_rainy_day
   OR m.is_snowy_day <> w.is_snowy_day
   OR m.temperature_max <> w.temperature_max
   OR m.temperature_min <> w.temperature_min
   OR m.temperature_mean <> w.temperature_mean
   OR m.precipitation_sum <> w.precipitation_sum
   OR m.snowfall_sum <> w.snowfall_sum;

-- 9.3) mart_zone_demand vs dim_zone
SELECT
    COUNT(*) AS mart_zone_demand_descriptor_mismatch_rows
FROM mart_zone_demand AS m
LEFT JOIN dim_zone AS z
    ON m.pickup_location_id = z.location_id
WHERE z.location_id IS NULL
   OR m.borough <> z.borough
   OR m.zone <> z.zone
   OR m.service_zone <> z.service_zone;


-- 10) RECONCILIATION WITH FACT
-- 10.1) mart_daily_demand
SELECT
    ROUND((SELECT SUM(trip_count) FROM mart_daily_demand), 2) AS mart_daily_demand_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips), 2) AS fact_row_count,
    ROUND((SELECT SUM(total_revenue) FROM mart_daily_demand), 2) AS mart_daily_demand_total_revenue_sum,
    ROUND((SELECT SUM(total_amount) FROM fact_taxi_trips), 2) AS fact_total_amount_sum,
    ROUND((SELECT SUM(negative_total_amount_trip_count) FROM mart_daily_demand), 2) AS mart_daily_demand_negative_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips WHERE total_amount < 0), 2) AS fact_negative_trip_count;

-- 10.2) mart_daily_payment_mix
SELECT
    ROUND((SELECT SUM(trip_count) FROM mart_daily_payment_mix), 2) AS mart_daily_payment_mix_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips), 2) AS fact_row_count,
    ROUND((SELECT SUM(total_revenue) FROM mart_daily_payment_mix), 2) AS mart_daily_payment_mix_total_revenue_sum,
    ROUND((SELECT SUM(total_amount) FROM fact_taxi_trips), 2) AS fact_total_amount_sum;

-- 10.3) mart_weather_impact
SELECT
    ROUND((SELECT SUM(trip_count) FROM mart_weather_impact), 2) AS mart_weather_impact_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips), 2) AS fact_row_count,
    ROUND((SELECT SUM(total_revenue) FROM mart_weather_impact), 2) AS mart_weather_impact_total_revenue_sum,
    ROUND((SELECT SUM(total_amount) FROM fact_taxi_trips), 2) AS fact_total_amount_sum,
    ROUND((SELECT SUM(negative_total_amount_trip_count) FROM mart_weather_impact), 2) AS mart_weather_impact_negative_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips WHERE total_amount < 0), 2) AS fact_negative_trip_count;

-- 10.4) mart_zone_demand
SELECT
    ROUND((SELECT SUM(trip_count) FROM mart_zone_demand), 2) AS mart_zone_demand_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips), 2) AS fact_row_count,
    ROUND((SELECT SUM(total_revenue) FROM mart_zone_demand), 2) AS mart_zone_demand_total_revenue_sum,
    ROUND((SELECT SUM(total_amount) FROM fact_taxi_trips), 2) AS fact_total_amount_sum,
    ROUND((SELECT SUM(negative_total_amount_trip_count) FROM mart_zone_demand), 2) AS mart_zone_demand_negative_trip_count_sum,
    ROUND((SELECT COUNT(*) FROM fact_taxi_trips WHERE total_amount < 0), 2) AS fact_negative_trip_count;


-- 11) MART ROW COUNT RECONCILIATION
SELECT
    (SELECT COUNT(*) FROM mart_daily_demand) AS mart_daily_demand_row_count,
    (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT pickup_date
            FROM fact_taxi_trips
        )
    ) AS expected_daily_demand_row_count;

SELECT
    (SELECT COUNT(*) FROM mart_daily_payment_mix) AS mart_daily_payment_mix_row_count,
    (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT
                pickup_date,
                COALESCE(payment_type_code, -1) AS payment_type_code_norm
            FROM fact_taxi_trips
        )
    ) AS expected_payment_mix_row_count;

SELECT
    (SELECT COUNT(*) FROM mart_weather_impact) AS mart_weather_impact_row_count,
    (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT pickup_date
            FROM fact_taxi_trips
        )
    ) AS expected_weather_impact_row_count;

SELECT
    (SELECT COUNT(*) FROM mart_zone_demand) AS mart_zone_demand_row_count,
    (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT pickup_date, pickup_location_id
            FROM fact_taxi_trips
        )
    ) AS expected_zone_demand_row_count;


-- 12) PAYMENT MIX SHARE CHECKS
-- 12.1) trip share sums
SELECT
    pickup_date,
    ROUND(SUM(payment_trip_share_pct), 6) AS trip_share_sum
FROM mart_daily_payment_mix
GROUP BY pickup_date
HAVING ABS(SUM(payment_trip_share_pct) - 1.0) > 0.0001
ORDER BY pickup_date;

-- 12.2) revenue share sums
SELECT
    pickup_date,
    ROUND(SUM(payment_revenue_share_pct), 6) AS revenue_share_sum,
    ROUND(SUM(total_revenue), 2) AS daily_revenue_sum
FROM mart_daily_payment_mix
GROUP BY pickup_date
HAVING ABS(SUM(total_revenue)) > 0.01
   AND ABS(SUM(payment_revenue_share_pct) - 1.0) > 0.0001
ORDER BY pickup_date;

-- 12.3) payment_type_name mapping sanity
SELECT
    pickup_date,
    payment_type_code,
    payment_type_name,
    COUNT(*) AS row_count
FROM mart_daily_payment_mix
GROUP BY pickup_date, payment_type_code, payment_type_name
ORDER BY pickup_date, payment_type_code
LIMIT 40;


-- 13) BUSINESS SNAPSHOT - MART_DAILY_DEMAND
SELECT
    pickup_date,
    trip_count,
    total_revenue,
    avg_trip_distance,
    avg_trip_duration_minutes,
    negative_total_amount_trip_count
FROM mart_daily_demand
ORDER BY pickup_date
LIMIT 31;


-- 14) BUSINESS SNAPSHOT - MART_DAILY_PAYMENT_MIX
SELECT
    pickup_date,
    payment_type_code,
    payment_type_name,
    trip_count,
    total_revenue,
    payment_trip_share_pct,
    payment_revenue_share_pct
FROM mart_daily_payment_mix
ORDER BY pickup_date, trip_count DESC, payment_type_code
LIMIT 50;


-- 15) BUSINESS SNAPSHOT - MART_WEATHER_IMPACT
SELECT
    pickup_date,
    is_rainy_day,
    is_snowy_day,
    temperature_mean,
    precipitation_sum,
    trip_count,
    total_revenue,
    avg_trip_distance,
    avg_trip_duration_minutes,
    negative_total_amount_trip_count
FROM mart_weather_impact
ORDER BY pickup_date
LIMIT 31;


-- 16) BUSINESS SNAPSHOT - MART_ZONE_DEMAND
SELECT
    pickup_date,
    pickup_location_id,
    borough,
    zone,
    trip_count,
    total_revenue,
    negative_total_amount_trip_count
FROM mart_zone_demand
ORDER BY pickup_date, trip_count DESC
LIMIT 50;


-- 17) TOP ZONES - FULL PERIOD
SELECT
    pickup_location_id,
    borough,
    zone,
    SUM(trip_count) AS trip_count,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    SUM(negative_total_amount_trip_count) AS negative_total_amount_trip_count
FROM mart_zone_demand
GROUP BY
    pickup_location_id,
    borough,
    zone
ORDER BY trip_count DESC, total_revenue DESC
LIMIT 20;


-- 18) NEGATIVE TOTAL AMOUNT PROFILE
SELECT
    pickup_date,
    negative_total_amount_trip_count,
    total_revenue
FROM mart_daily_demand
WHERE negative_total_amount_trip_count > 0
ORDER BY pickup_date
LIMIT 31;