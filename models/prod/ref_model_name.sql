MODEL (
  name game.ref_model_name,
  kind VIEW,
  cron '@daily',
  validate_query FALSE
);

SELECT
  *
FROM @folder_env('__game.model_name')