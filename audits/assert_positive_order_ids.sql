AUDIT (
  name assert_positive_order_ids,
);

SELECT *
FROM @this_model
WHERE
  item_id < 0


-- run "sqlmesh audit" to execute all audits
-- this audit ensures there are no item_ids that return a negative value
-- will be executed after each run