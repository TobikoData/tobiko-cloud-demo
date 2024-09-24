MODEL (
  name tcloud_demo.deduplicate,
  kind VIEW,
  cron '@daily',
  grain date_week,
);

with deduplicated_data as (
@deduplicate(tcloud_demo.seed_model, [id, cast(event_date as date)], ['event_date DESC'])
)

select * from deduplicated_data
