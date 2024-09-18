MODEL (
  name preview_sandbox.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (
    assert_positive_order_ids
  )
);

-- command to generate unit test code in a yaml file
-- sqlmesh create_test preview_sandbox.incremental_model --query preview_sandbox.seed_model "select * from preview_sandbox.seed_model limit 5" 

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders
FROM preview_sandbox.incremental_model
GROUP BY
  item_id