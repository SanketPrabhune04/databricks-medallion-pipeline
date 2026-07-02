-- =====================================================================
-- 04 · GOLD — Star schema with surrogate keys + SCD Type 2
-- Fact + conformed dimensions, business-ready for analytics.
-- Build order: dimensions first, then fact (fact looks up dim keys).
-- =====================================================================

-- Dimensions with surrogate keys ------------------------------------
CREATE OR REPLACE TABLE workspace.gold.dim_product AS
SELECT ROW_NUMBER() OVER (ORDER BY product_id) AS product_key,
       product_id, product_name, category, brand, price
FROM workspace.silver.products;

CREATE OR REPLACE TABLE workspace.gold.dim_region AS
SELECT ROW_NUMBER() OVER (ORDER BY region_id) AS region_key, region_id, region
FROM workspace.silver.regions;

CREATE OR REPLACE TABLE workspace.gold.dim_date AS
SELECT DISTINCT CAST(DATE_FORMAT(order_date,'yyyyMMdd') AS INT) AS date_key,
       order_date AS full_date, YEAR(order_date) AS year, MONTH(order_date) AS month,
       QUARTER(order_date) AS quarter, DATE_FORMAT(order_date,'MMMM') AS month_name,
       DAY(order_date) AS day
FROM workspace.silver.orders WHERE order_date IS NOT NULL;

-- SCD Type 2 dimension: dim_customer --------------------------------
CREATE TABLE IF NOT EXISTS workspace.gold.dim_customer (
    customer_key BIGINT, customer_id STRING, full_name STRING, email STRING,
    city STRING, state STRING, effective_from DATE, effective_to DATE, is_current BOOLEAN
);

-- Step 1: close changed current versions
MERGE INTO workspace.gold.dim_customer AS t
USING workspace.silver.customers AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED AND (t.email<>s.email OR t.city<>s.city OR t.state<>s.state OR t.full_name<>s.full_name)
THEN UPDATE SET t.is_current=false, t.effective_to=current_date();

-- Step 2: insert new versions (new + changed), assign continuing surrogate keys
MERGE INTO workspace.gold.dim_customer AS t
USING (
    SELECT s.*,
           (SELECT COALESCE(MAX(customer_key),0) FROM workspace.gold.dim_customer)
             + ROW_NUMBER() OVER (ORDER BY s.customer_id) AS new_key
    FROM workspace.silver.customers s
    LEFT JOIN workspace.gold.dim_customer d ON s.customer_id=d.customer_id AND d.is_current=true
    WHERE d.customer_id IS NULL
) AS s
ON false
WHEN NOT MATCHED THEN INSERT
    (customer_key, customer_id, full_name, email, city, state, effective_from, effective_to, is_current)
    VALUES (s.new_key, s.customer_id, s.full_name, s.email, s.city, s.state, current_date(), NULL, true);

-- Fact table with surrogate-key lookups -----------------------------
CREATE OR REPLACE TABLE workspace.gold.fact_orders AS
SELECT o.order_id, c.customer_key, p.product_key,
       CAST(DATE_FORMAT(o.order_date,'yyyyMMdd') AS INT) AS date_key,
       o.order_date, o.quantity, o.total_amount
FROM workspace.silver.orders o
LEFT JOIN workspace.gold.dim_customer c ON o.customer_id=c.customer_id AND c.is_current=true
LEFT JOIN workspace.gold.dim_product  p ON o.product_id=p.product_id;

-- Business aggregate ------------------------------------------------
CREATE OR REPLACE TABLE workspace.gold.sales_by_category AS
SELECT p.category, SUM(f.total_amount) AS revenue, SUM(f.quantity) AS units
FROM workspace.gold.fact_orders f
JOIN workspace.gold.dim_product p ON f.product_key = p.product_key
GROUP BY p.category ORDER BY revenue DESC;
