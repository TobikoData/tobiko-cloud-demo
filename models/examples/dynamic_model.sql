MODEL (
  name tcloud_demo.dynamic_model,
);

with dynamic_model as (
  @dynamic_columns()
)

select * from dynamic_model;