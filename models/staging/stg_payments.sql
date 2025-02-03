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
  'advanced_cll_column' as advanced_cll_column,
  amount / 100 AS amount, /* `amount` is currently stored in cents, so we convert it to dollars */
  'new_column' AS new_column_V3, /* non-breaking change example  */
  'new_column' AS new_column_v2, /* non-breaking change example  */
  'new_column' AS new_column_v4, /* non-breaking change example  */
  'new_column' AS new_column_v5, /* non-breaking change example  */
  -- 'new_column' AS new_column_v6, /* non-breaking change example  */
FROM tcloud_demo.seed_raw_payments

-- how to generate unit test code without manually writing yaml by hand
-- this will generate a file in the tests/ folder: test_stg_payments.yaml
-- sqlmesh create_test tcloud_demo.stg_payments --query tcloud_demo.seed_raw_payments "select * from tcloud_demo.seed_raw_payments limit 5" 