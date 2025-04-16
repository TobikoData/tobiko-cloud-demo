MODEL (
  name tcloud_demo.model_b,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date)
);

SELECT
  item_id,
  event_date,
  new_column,
  'new_column_2' AS new_column_2,
  'new_column_3' AS new_column_3,
  'new_column_4' AS new_column_4
FROM tcloud_demo.model_a
WHERE
  event_date BETWEEN @start_date AND @end_date