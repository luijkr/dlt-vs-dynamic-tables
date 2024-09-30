USE SCHEMA <database>.<schema>;

CREATE OR REPLACE DYNAMIC TABLE successful_orders
    LAG='DOWNSTREAM'
    WAREHOUSE=<warehouse>
AS
SELECT
  o.order_id,
  o.customer_id,
  o.product_id,
  o.order_date,
  o.quantity
FROM orders o
JOIN transactions t
  ON o.order_id = t.order_id
WHERE t.transaction_status = 'Success';

CREATE OR REPLACE DYNAMIC TABLE customer_sales
    LAG='1 MINUTE'
    WAREHOUSE=<warehouse>
AS
SELECT
  c.customer_id,
  COUNT(1) as num_orders,
  SUM(p.price) as total_spent
FROM successful_orders o
JOIN customers c
  ON o.customer_id = c.customer_id
JOIN products p
  ON o.product_id = p.product_id
GROUP BY ALL;

CREATE OR REPLACE DYNAMIC TABLE product_sales_by_category
    LAG='1 MINUTE'
    WAREHOUSE=<warehouse>
AS
SELECT
  p.product_id,
  p.product_name,
  p.category as product_category,
  COUNT(1) as num_sales,
  SUM(p.price) as revenue
FROM successful_orders o
JOIN products p
  ON o.product_id = p.product_id
GROUP BY ALL;