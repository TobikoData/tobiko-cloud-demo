MODEL (
  name tcloud_demo.incremental_events,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (event_timestamp, '%Y-%m-%d'), /* DELETE by time range, then INSERT */
    lookback 2 /* handle late arriving events for the past 2 (2*1) days based on cron interval */
  ) /* forward_only true -- All changes will be forward only */,
  start '2024-06-17',
  cron '@daily',
  grain event_id,
  stamp 'demo-sung', /* should be unique every time */
  audits (
    UNIQUE_VALUES(
      columns = (
        event_id
      ) /* data audit tests only run for the evaluated intervals */
    ),
    NOT_NULL(columns = (
      event_id
    ))
  )
);

/* How to work with incremental forward only models */ /* step 1: `sqlmesh plan dev` to create this model for the first time and backfill for all of history */ /* step 2: change the user_intent_level conditional value */ /* step 3: pick a start date to backfill like: '2024-06-18' */ /* step 4: validate only a portion of rows were backfilled: sqlmesh fetchdf "select * from tcloud_demo__dev.incremental_events" */ /* step 5: `sqlmesh plan` to promote to prod with a virtual update, note: the dev backfill preview won't be reused for promotion and is only for dev purposes */ /* step 6: sqlmesh plan --restate-model "tcloud_demo.incremental_events", to invoke a backfill to mirror dev's data preview */ /* step 7: pick the same backfill start date for prod as dev's above: '2024-06-18' */ /* step 8: validate changes to prod: sqlmesh fetchdf "select * from tcloud_demo.incremental_events" */ /* Note: by default, only complete intervals are processed, so if today was 2024-06-21 and the day isn't over, it would NOT backfill the day's interval of data because it's not complete */
SELECT
  event_id,
  5 AS dummy_id,
  event_name,
  event_timestamp::DATE AS event_timestamp,
  user_id,
  IF(event_name = 'blog_view', 'high', 'low') AS user_intent_level
FROM sqlmesh-public-demo.tcloud_raw_data.raw_events
WHERE
  event_timestamp BETWEEN @start_ds AND @end_ds;

@measure(
  SELECT
    event_timestamp::DATE AS ts, /* Custom measure time column `ts` */
    COUNT(*) AS daily_row_count, /* Daily row count */
    COUNT(DISTINCT event_name) AS unique_event_name_count /* Count unique event_name values */
  FROM tcloud_demo.incremental_events
  WHERE
    event_timestamp BETWEEN @start_ds AND @end_ds
  GROUP BY
    event_timestamp /* Group measure by time */
) /* track observer metrics with plain SQL */;

/* you can use macros to dynamically track  metrics you care about */
@DEF(event_names, ['page_view', 'product_view', 'ad_view', 'video_view', 'blog_view']);

@measure(
  SELECT
    event_timestamp::DATE AS ts,
    @EACH(@event_names, x -> COUNT(CASE WHEN event_name = x THEN 1 END) AS @{x}_count)
  FROM tcloud_demo.incremental_events
  WHERE
    event_timestamp::DATE BETWEEN @start_ds AND @end_ds
  GROUP BY
    event_timestamp::DATE
)