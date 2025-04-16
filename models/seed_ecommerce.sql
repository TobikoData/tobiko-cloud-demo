MODEL (
  name demo.seed_ecommerce,
  kind SEED (
    path '../seeds/ecommerce.csv'
  ),
  columns (
    customer_id INT64,
    order_id INT64,
    product_name STRING(50),
    product_category STRING(50),
    purchase_amount FLOAT64,
    purchase_date DATE,
    country STRING(50),
    customer_persona STRING(50)
  ),
  grain (customer_id, order_id)
)