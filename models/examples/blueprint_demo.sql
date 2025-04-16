MODEL (
  name @customer.orders,
  kind VIEW,
  cron '@daily',
  blueprints (
    (customer := customer1, paid_field := customer_persona, customer_filter := 1), /* each variable is a string */
    (customer := customer2, paid_field := customer_persona, customer_filter := 2),
    (customer := customer3, paid_field := customer_persona, customer_filter := 3),
    (customer := customer4, paid_field := customer_persona, customer_filter := 4),
    (customer := customer5, paid_field := customer_persona, customer_filter := 5)
  ),
  audits (
    unique_combination_of_columns(columns := (customer_id, order_id)),
    NOT_NULL(columns := (customer_id, order_id))
  )
);

SELECT
  customer_id,
  order_id,
  product_name,
  product_category,
  purchase_amount,
  purchase_date,
  country,
  @{paid_field} AS customer_persona /* use `@{variable}` syntax to make sqlmesh interpret the variable as a column */
FROM demo.seed_ecommerce
WHERE
  customer_id = @customer_filter::INT64 /* we do `WHERE @condition` vs. `FROM @condition` to repsect the AST */