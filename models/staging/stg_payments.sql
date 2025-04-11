MODEL (
  name tcloud_demo.stg_payments,
  cron '@daily',
  grains payment_id,
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
  'advanced_cll_column' AS advanced_cll_column,
  amount / 100 AS amount, /* `amount` is currently stored in cents, so we convert it to dollars */
  -- 'new_column' AS new_column_from_afzal /* non-breaking change example  */ 
FROM tcloud_demo.seed_raw_payments  

-- how to generate unit test code without manually writing yaml by hand
-- this will generate a file in the tests/ folder: test_stg_payments.yaml
-- tcloud sqlmesh create_test tcloud_demo.stg_payments --query tcloud_demo.seed_raw_payments "select * from tcloud_demo.seed_raw_payments limit 5" 

-- table_diff - "tcloud sqlmesh table_diff afzal_demo:prod tcloud_demo.stg_payments"

-- sql transpilation "tcloud sqlmesh render --dialect databricks tcloud_demo.stg_payments"

-- tcloud sqlmesh fetchdf "select * from tcloud_demo__afzal_demo.stg_payments"