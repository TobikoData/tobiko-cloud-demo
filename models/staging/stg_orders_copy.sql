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

select * from
(@deduplicate(tcloud_demo.seed_raw_orders, [id, cast(order_date as date)], ['order_date desc', 'status asc']))