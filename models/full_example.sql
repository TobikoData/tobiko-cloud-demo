MODEL (
  name tcloud_demo.full_wap,
  kind FULL,
  start '2024-06-17',
  cron '@daily',
  grain event_id,
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
  event_timestamp BETWEEN @start_date_wap() AND @end_date_wap();
