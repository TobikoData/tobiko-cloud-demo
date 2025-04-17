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
  '34' AS new_column_demos /* non-breaking change example  */
FROM tcloud_demo.seed_raw_payments