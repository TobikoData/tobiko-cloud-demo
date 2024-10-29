MODEL (
  name tcloud_demo.incremental_demo,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (transaction_timestamp, '%Y-%m-%d %H:%M:%S'), -- How does this behave? DELETE by time range, then INSERT
    lookback 2, --How do I handle late arriving data? Handle late arriving events for the past 2 (2*1) days based on cron interval. So each time it runs, it'll process today, yesterday, and day before yesterday
  ),
  start '2024-10-25', --This is as far back I want to care about backfilling my data
  cron '@daily', --What schedule should I run these at? Daily at Midnight UTC
  grain transaction_id,
  --allow_partials true, --What tradeoffs am I willing to make for fresh data? I'm only okay with allowing partial intervals to be processed for things like logging event data, but for sales and product data, I want to make sure complete intervals are processed so end users don't confuse incomplete data with incorrect data
  audits (UNIQUE_VALUES(columns = ( --How do I test this data? data audit tests only run for the processed intervals
    transaction_id
  )), NOT_NULL(columns = (
    transaction_id
  )))
);

WITH sales_data AS (
  SELECT
    transaction_id,
    product_id,
    customer_id,
    transaction_amount,
    transaction_timestamp, --How do I account for UTC vs. PST (California baby) timestamps, do I convert them? Make sure all time columns are in UTC and then convert them to PST in the presentation layer downstream
    payment_method,
    currency
  FROM sqlmesh-public-demo.tcloud_raw_data.sales  -- Source A
  WHERE transaction_timestamp BETWEEN @start_dt AND @end_dt --How do I make this run fast and only the intervals necessary (read: partitions)? Use our date macros that will automatically run the intervals necessary. And because SQLMesh manages state, it will know what needs to run each time you invoke `sqlmesh run`
),

product_usage AS (
  SELECT
    product_id,
    customer_id,
    last_usage_date,
    usage_count,
    feature_utilization_score,
    user_segment
  FROM sqlmesh-public-demo.tcloud_raw_data.product_usage  -- Source B
  WHERE last_usage_date BETWEEN DATE_SUB(@start_dt, INTERVAL 30 DAY) AND @end_dt  -- Include recent usage data
),

final AS (
  SELECT
    s.transaction_id,
    s.product_id,
    s.customer_id,
    s.transaction_amount,
    s.transaction_timestamp,
    -- DATETIME(s.transaction_timestamp, 'America/Los_Angeles') as transaction_timestamp_pst, --Convert this to PST using a SQL function
    s.payment_method,
    s.currency,
    -- Product usage metrics
    p.last_usage_date,
    p.usage_count,
    p.feature_utilization_score,
    p.user_segment,
    -- Derived metrics
    CASE 
      WHEN p.usage_count > 100 AND p.feature_utilization_score > 0.8 THEN 'Power User'
      WHEN p.usage_count > 50 THEN 'Regular User'
      WHEN p.usage_count IS NULL THEN 'New User'
      ELSE 'Light User'
    END as user_type,
    -- Time since last usage
    DATE_DIFF(s.transaction_timestamp, p.last_usage_date, DAY) as days_since_last_usage
  FROM sales_data s
  LEFT JOIN product_usage p
    ON s.product_id = p.product_id 
    AND s.customer_id = p.customer_id
)

SELECT * FROM final