CREATE OR REFRESH STREAMING LIVE TABLE successful_orders
AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  o.order_date,
  o.quantity
FROM STREAM <database>.<schema>.orders o
JOIN <database>.<schema>.transactions t
  ON o.order_id = t.order_id
WHERE t.transaction_status = 'Success';

CREATE OR REFRESH MATERIALIZED VIEW customer_sales
AS
SELECT
  c.customer_id,
  COUNT(1) as num_orders,
  SUM(p.price) as total_spent
FROM LIVE.successful_orders o
JOIN <database>.<schema>.customers c
  ON o.customer_id = c.customer_id
JOIN <database>.<schema>.products p
  ON o.product_id = p.product_id
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW product_sales_by_category
AS
SELECT
  p.product_id,
  p.product_name,
  p.category as product_category,
  COUNT(1) as num_sales,
  SUM(p.price) as revenue
FROM LIVE.successful_orders o
JOIN <database>.<schema>.products p
  ON o.product_id = p.product_id
GROUP BY ALL;