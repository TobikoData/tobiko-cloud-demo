MODEL (
  name preview_sandbox.seed_model,
  kind SEED (
    path '../../seeds/seed_data.csv'
  ),
  columns (
    id INT64,
    item_id INT64,
    event_date DATE
  ),
  grain (id, event_date)
)