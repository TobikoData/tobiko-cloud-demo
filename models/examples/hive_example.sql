MODEL (
  name tcloud_demo.hive_example,
  cron '@daily',
  grain customer_id,
  audits (UNIQUE_VALUES(columns = (
    customer_id
  )), NOT_NULL(columns = (
    customer_id
  )))
);

WITH customer_data AS (
  SELECT
    *,
    CONCAT(first_name, ' ', last_name) AS full_name,
    DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration
  FROM tcloud_demo.stg_customers
), order_data AS (
  SELECT
    *,
    CASE
      WHEN order_status = 'completed' THEN 1
      ELSE 0
    END AS is_completed
  FROM tcloud_demo.stg_orders
), product_data AS (
  SELECT
    *,
    REGEXP_EXTRACT(product_name, '^([A-Za-z]+)', 1) AS product_category
  FROM tcloud_demo.stg_products
), customer_orders AS (
  SELECT
    c.customer_id,
    c.full_name,
    c.days_since_registration,
    COUNT(o.order_id) AS total_orders,
    SUM(o.is_completed) AS completed_orders,
    COLLECT_SET(p.product_category) AS purchased_categories
  FROM customer_data c
  LEFT JOIN order_data o ON c.customer_id = o.customer_id
  LEFT JOIN product_data p ON o.product_id = p.product_id
  GROUP BY
    c.customer_id,
    c.full_name,
    c.days_since_registration
), customer_stats AS (
  SELECT
    customer_id,
    full_name,
    days_since_registration,
    total_orders,
    completed_orders,
    SIZE(purchased_categories) AS unique_categories,
    PERCENTILE(total_orders, 0.5) OVER () AS median_total_orders,
    NTILE(4) OVER (ORDER BY total_orders) AS order_quartile
  FROM customer_orders
), final AS (
  SELECT
    cs.*,
    CASE
      WHEN cs.total_orders > cs.median_total_orders THEN 'High'
      WHEN cs.total_orders = cs.median_total_orders THEN 'Average'
      ELSE 'Low'
    END AS order_frequency,
    CASE
      WHEN cs.days_since_registration > 365 AND cs.total_orders > 10 THEN 'Loyal'
      WHEN cs.days_since_registration <= 30 THEN 'New'
      ELSE 'Regular'
    END AS customer_status,
    EXPLODE(co.purchased_categories) AS category
  FROM customer_stats cs
  JOIN customer_orders co ON cs.customer_id = co.customer_id
)
SELECT
  customer_id,
  full_name,
  days_since_registration,
  total_orders,
  completed_orders,
  unique_categories,
  order_frequency,
  customer_status,
  order_quartile,
  COLLECT_LIST(category) AS favorite_categories
FROM final
GROUP BY
  customer_id,
  full_name,
  days_since_registration,
  total_orders,
  completed_orders,
  unique_categories,
  order_frequency,
  customer_status,
  order_quartile
CLUSTER BY customer_status, order_quartile
