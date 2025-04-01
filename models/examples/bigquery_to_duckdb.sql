MODEL (
  name demo.bigquery_to_duckdb,
  cron '@daily',
);


SELECT STRUCT(
  'value1' AS key1,
  42 AS key2
) AS struct_col;
