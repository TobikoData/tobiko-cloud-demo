MODEL (
  name tcloud_demo.date_spine,
  kind VIEW,
  cron '@daily',
  grain date_week,
);

WITH discount_promotion_dates AS (
  @date_spine('week', '2024-01-01', '2024-10-16')
)

SELECT * FROM discount_promotion_dates
