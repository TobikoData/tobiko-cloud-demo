MODEL (
  name tcloud_demo.customers,
  cron '@daily',
  grain customer_id,
  audits (
    UNIQUE_VALUES(columns = (
      customer_id
    )),
    NOT_NULL(columns = (
      customer_id
    ))
  )
);

WITH customers AS (
  SELECT
    *
  FROM tcloud_demo.stg_customers
), orders AS (
  SELECT
    *
  FROM tcloud_demo.stg_orders
), payments AS (
  SELECT
    payment_id,
    order_id,
    payment_method,
    amount
  FROM tcloud_demo.stg_payments
), customer_orders AS (
  SELECT
    customer_id,
    MIN(order_date) AS first_order,
    MAX(order_date) AS most_recent_order,
    COUNT(order_id) AS number_of_orders
  FROM orders
  GROUP BY
    customer_id
), customer_payments AS (
  SELECT
    orders.customer_id,
    SUM(amount) AS total_amount
  FROM payments
  LEFT JOIN orders
    ON payments.order_id = orders.order_id
  GROUP BY
    orders.customer_id
), final AS (
  SELECT
    customers.customer_id, /* these inline comments cascade to the data catalog automatically */ /* This is a unique identifier for a customer */
    customers.first_name, /* Customer's first name. PII. */
    customers.last_name, /* Customer's last name. PII. */
    customer_orders.first_order, /* Date (UTC) of a customer's first order */
    customer_orders.most_recent_order, /* Date (UTC) of a customer's most recent order */
    customer_orders.number_of_orders, /* Count of the number of orders a customer has placed */
    customer_payments.total_amount AS customer_lifetime_value /* Total value (AUD) of a customer's orders */
  FROM customers
  LEFT JOIN customer_orders
    ON customers.customer_id = customer_orders.customer_id
  LEFT JOIN customer_payments
    ON customers.customer_id = customer_payments.customer_id
)
SELECT
  *
FROM final