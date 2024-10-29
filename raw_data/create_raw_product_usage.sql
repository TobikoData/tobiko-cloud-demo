-- Create the table with appropriate schema
CREATE TABLE IF NOT EXISTS `sqlmesh-public-demo.tcloud_raw_data.product_usage` (
  product_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  last_usage_date TIMESTAMP NOT NULL,
  usage_count INT64 NOT NULL,
  feature_utilization_score FLOAT64 NOT NULL,
  user_segment STRING NOT NULL,
);

-- Insert the data
INSERT INTO `sqlmesh-public-demo.tcloud_raw_data.product_usage`
(product_id, customer_id, last_usage_date, usage_count, feature_utilization_score, user_segment)
VALUES
  ('PROD-101', 'CUST-001', TIMESTAMP '2024-10-25 23:45:00+00:00', 120, 0.85, 'enterprise'),
  ('PROD-103', 'CUST-001', TIMESTAMP '2024-10-27 12:30:00+00:00', 95, 0.75, 'enterprise'),
  ('PROD-102', 'CUST-002', TIMESTAMP '2024-10-25 15:15:00+00:00', 150, 0.92, 'enterprise'),
  ('PROD-103', 'CUST-002', TIMESTAMP '2024-10-26 14:20:00+00:00', 80, 0.68, 'enterprise'),
  ('PROD-101', 'CUST-003', TIMESTAMP '2024-10-25 18:30:00+00:00', 45, 0.45, 'professional'),
  ('PROD-102', 'CUST-003', TIMESTAMP '2024-10-27 19:45:00+00:00', 30, 0.35, 'professional'),
  ('PROD-103', 'CUST-004', TIMESTAMP '2024-10-25 21:20:00+00:00', 15, 0.25, 'starter'),
  ('PROD-102', 'CUST-005', TIMESTAMP '2024-10-25 23:10:00+00:00', 5, 0.15, 'starter'),
  ('PROD-102', 'CUST-006', TIMESTAMP '2024-10-26 15:30:00+00:00', 110, 0.88, 'enterprise'),
  ('PROD-101', 'CUST-007', TIMESTAMP '2024-10-26 17:45:00+00:00', 60, 0.55, 'professional'),
  ('PROD-103', 'CUST-008', TIMESTAMP '2024-10-26 22:20:00+00:00', 25, 0.30, 'starter'),
  ('PROD-101', 'CUST-009', TIMESTAMP '2024-10-27 05:15:00+00:00', 75, 0.65, 'professional'),
  ('PROD-102', 'CUST-010', TIMESTAMP '2024-10-27 08:40:00+00:00', 3, 0.10, 'starter');
