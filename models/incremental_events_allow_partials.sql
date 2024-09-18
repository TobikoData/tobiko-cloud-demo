MODEL (
  name preview_sandbox.incremental_events_allow_partials,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column (event_timestamp, '%Y-%m-%d'), -- DELETE by time range, then INSERT
    forward_only true, -- All changes will be forward only
  ),
  start '2024-06-17',
  cron '@daily',
  grain event_id,
  allow_partials true, --ignores cron
  audits (UNIQUE_VALUES(columns = ( -- data audit tests only run for the evaluated intervals
    event_id
  )), NOT_NULL(columns = (
    event_id
  )))
);

-- How to work with incremental forward only models
-- step 1: `sqlmesh plan dev` to create this model for the first time and backfill for all of history
-- step 2: change the user_intent_level conditional value
-- step 3: pick a start date to backfill like: '2024-06-18'
-- step 4: validate only a portion of rows were backfilled: sqlmesh fetchdf "select * from tcloud_demo__dev.incremental_events"
-- step 5: `sqlmesh plan` to promote to prod with a virtual update, note: the dev backfill preview won't be reused for promotion and is only for dev purposes
-- step 6: sqlmesh plan --restate-model "preview_sandbox.incremental_events", to invoke a backfill to mirror dev's data preview
-- step 7: pick the same backfill start date for prod as dev's above: '2024-06-18'
-- step 8: validate changes to prod: sqlmesh fetchdf "select * from preview_sandbox.incremental_events"
-- Note: by default, only complete intervals are processed, so if today was 2024-06-21 and the day isn't over, it would NOT backfill the day's interval of data because it's not complete

SELECT
  event_id,
  event_name,
  event_timestamp::date as event_timestamp,
  user_id,
  IF(event_name = 'blog_view', 'high', 'low') AS user_intent_level,
FROM sqlmesh-public-demo.tcloud_raw_data.raw_events --external model, automatically generate yaml using command: `sqlmesh create_external_models`
WHERE
  event_timestamp BETWEEN @start_ds AND @end_ds -- use the correct time format: https://sqlmesh.readthedocs.io/en/stable/concepts/macros/macro_variables/#temporal-variables
