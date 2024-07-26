MODEL (
  name tcloud_demo.stg_orders_copy,
  cron '@daily',
  grain order_id,
  audits (UNIQUE_VALUES(columns = (
    id
  )), NOT_NULL(columns = (
    id
  )))
);

with raw_data as (
  select * from tcloud_demo.seed_raw_orders
)

select * from
(@deduplicate(raw_data, [id, cast(order_date as timestamp)], ['order_date desc', 'status asc']))