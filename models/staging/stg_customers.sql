MODEL (
  name preview_sandbox.stg_customers,
  cron '@daily',
  grain org_id,
  audits (UNIQUE_VALUES(columns = (
    customer_id
  )), NOT_NULL(columns = (
    customer_id
  )))
);

SELECT
  id AS customer_id,
  first_name,
  last_name,
  1 AS new_column /* non-breaking change */
FROM preview_sandbox.seed_raw_customers