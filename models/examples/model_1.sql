MODEL (
  name tcloud_demo.model_1,
  kind VIEW,
  cron '@daily',
  grain item_id,
  audits (
    UNIQUE_VALUES(
      columns = (
        item_id
      ) /* data audit tests only run for the evaluated intervals */
    ),
    NOT_NULL(columns = (
      item_id
    ))
  )
);

/* run this command to synchronize multiple repos: tcloud sqlmesh -p . -p repo_b/ plan */
SELECT
  1 AS item_id, /* breaking change, will backfill repo_b.model_2 but fail */
  3 AS new_column,
  3 AS hello, /* non breaking change */
  CURRENT_DATE AS event_date