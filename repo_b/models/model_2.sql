MODEL (
  name repo_b.model_2,
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
  'new_column_4' AS new_column_4 -- repo b only breaking change
FROM tcloud_demo.model_1
WHERE
  event_date BETWEEN @start_date AND @end_date