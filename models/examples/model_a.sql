MODEL (
  name tcloud_demo.model_a,
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

/* command to generate unit test code in a yaml file */ /* sqlmesh create_test tcloud_demo.incremental_model --query tcloud_demo.seed_model "select * from tcloud_demo.seed_model limit 5" */
SELECT
  1 AS item_id,
  3 AS new_column,
  CURRENT_DATE AS event_date