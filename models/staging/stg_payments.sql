MODEL (
  name tcloud_demo.stg_payments,
  cron '@daily',
  grain payment_id,
  audits (UNIQUE_VALUES(columns = (
    payment_id
  )), NOT_NULL(columns = (
    payment_id
  )))
);

SELECT
  id AS payment_id,
  order_id,
  payment_method,
  amount / 100 AS amount, /* `amount` is currently stored in cents, so we convert it to dollars */
  'example_column' AS example_column, /* advanced change categorization example, in open source this will force an indirect breacking change on all downstream models, with Tobiko Cloud it will make this an indirect non-breaking change where this column is NOT referenced downstream */
  -- 'new_column' AS new_column_from_sung, /* non-breaking change example  */
FROM tcloud_demo.seed_raw_payments

-- how to generate unit test code without manually writing yaml by hand
-- this will generate a file in the tests/ folder: test_stg_payments.yaml
-- sqlmesh create_test tcloud_demo.stg_payments --query tcloud_demo.seed_raw_payments "select * from tcloud_demo.seed_raw_payments limit 5" 
-- Check out this debugger view in Tobiko Cloud: https://cloud.tobikodata.com/sqlmesh/tobiko/public-demo/observer/environments/prod/runs/dac3b282a79d4188a5e3ee5e7eca696a