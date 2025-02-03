MODEL (
  name tcloud_demo.model_a,
  kind VIEW,
  cron '@daily',
  grain item_id,
  audits (UNIQUE_VALUES(columns = ( -- data audit tests only run for the evaluated intervals
    item_id
  )), NOT_NULL(columns = (
    item_id
  )))
);

-- command to generate unit test code in a yaml file
-- sqlmesh create_test tcloud_demo.incremental_model --query tcloud_demo.seed_model "select * from tcloud_demo.seed_model limit 5" 

SELECT
  1 as item_id,
  3 as new_column,
  CURRENT_DATE as event_date