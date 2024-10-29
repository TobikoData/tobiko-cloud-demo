-- First, create the table with appropriate schema
CREATE TABLE IF NOT EXISTS `sqlmesh-public-demo.tcloud_raw_data.sales` (
  transaction_id STRING NOT NULL,
  product_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  transaction_amount NUMERIC(10,2) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  payment_method STRING,
  currency STRING,
);

-- Then, insert the data
INSERT INTO `sqlmesh-public-demo.tcloud_raw_data.sales`
(transaction_id, product_id, customer_id, transaction_amount, transaction_timestamp, payment_method, currency)
VALUES
  ('TX-001', 'PROD-101', 'CUST-001', 99.99, TIMESTAMP '2024-10-25 08:30:00+00:00', 'credit_card', 'USD'),
  ('TX-002', 'PROD-102', 'CUST-002', 149.99, TIMESTAMP '2024-10-25 09:45:00+00:00', 'paypal', 'USD'),
  ('TX-003', 'PROD-101', 'CUST-003', 99.99, TIMESTAMP '2024-10-25 15:20:00+00:00', 'credit_card', 'USD'),
  ('TX-004', 'PROD-103', 'CUST-004', 299.99, TIMESTAMP '2024-10-25 18:10:00+00:00', 'credit_card', 'USD'),
  ('TX-005', 'PROD-102', 'CUST-005', 149.99, TIMESTAMP '2024-10-25 21:30:00+00:00', 'debit_card', 'USD'),
  ('TX-006', 'PROD-101', 'CUST-001', 99.99, TIMESTAMP '2024-10-26 03:15:00+00:00', 'credit_card', 'USD'),
  ('TX-007', 'PROD-103', 'CUST-002', 299.99, TIMESTAMP '2024-10-26 07:45:00+00:00', 'paypal', 'USD'),
  ('TX-008', 'PROD-102', 'CUST-006', 149.99, TIMESTAMP '2024-10-26 11:20:00+00:00', 'credit_card', 'USD'),
  ('TX-009', 'PROD-101', 'CUST-007', 99.99, TIMESTAMP '2024-10-26 14:30:00+00:00', 'debit_card', 'USD'),
  ('TX-010', 'PROD-103', 'CUST-008', 299.99, TIMESTAMP '2024-10-26 19:45:00+00:00', 'credit_card', 'USD'),
  ('TX-011', 'PROD-101', 'CUST-009', 99.99, TIMESTAMP '2024-10-27 02:30:00+00:00', 'paypal', 'USD'),
  ('TX-012', 'PROD-102', 'CUST-010', 149.99, TIMESTAMP '2024-10-27 05:15:00+00:00', 'credit_card', 'USD'),
  ('TX-013', 'PROD-103', 'CUST-001', 299.99, TIMESTAMP '2024-10-27 08:40:00+00:00', 'credit_card', 'USD'),
  ('TX-014', 'PROD-101', 'CUST-002', 99.99, TIMESTAMP '2024-10-27 13:25:00+00:00', 'debit_card', 'USD'),
  ('TX-015', 'PROD-102', 'CUST-003', 149.99, TIMESTAMP '2024-10-27 16:50:00+00:00', 'credit_card', 'USD');