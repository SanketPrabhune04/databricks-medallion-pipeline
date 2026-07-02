-- =====================================================================
-- 02 · SILVER — Clean, type, dedup, incremental upsert
-- Silver holds the trustworthy current state: one clean row per key.
-- =====================================================================

-- Target table shape (needed for MERGE) -----------------------------
CREATE TABLE IF NOT EXISTS workspace.silver.orders (
    order_id STRING, customer_id STRING, product_id STRING,
    order_date DATE, quantity INT, total_amount DECIMAL(12,2), _updated_at TIMESTAMP
);

-- Incremental MERGE: clean + dedup (latest wins) + upsert -----------
MERGE INTO workspace.silver.orders AS t
USING (
    SELECT order_id, customer_id, product_id, order_date, quantity, total_amount
    FROM (
        SELECT order_id, customer_id, product_id,
               CAST(order_date AS DATE)          AS order_date,
               CAST(quantity AS INT)             AS quantity,
               CAST(total_amount AS DECIMAL(12,2)) AS total_amount,
               ROW_NUMBER() OVER (PARTITION BY order_id
                                  ORDER BY _batch_id DESC, _ingested_at DESC) AS rn
        FROM workspace.bronze.orders
        WHERE order_id IS NOT NULL AND quantity > 0 AND total_amount >= 0
    ) WHERE rn = 1
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    t.customer_id=s.customer_id, t.product_id=s.product_id, t.order_date=s.order_date,
    t.quantity=s.quantity, t.total_amount=s.total_amount, t._updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT
    (order_id, customer_id, product_id, order_date, quantity, total_amount, _updated_at)
    VALUES (s.order_id, s.customer_id, s.product_id, s.order_date, s.quantity, s.total_amount, current_timestamp());

-- Conformed dimensions (full refresh) -------------------------------
CREATE OR REPLACE TABLE workspace.silver.customers AS
SELECT DISTINCT customer_id,
    CONCAT(TRIM(first_name),' ',TRIM(last_name)) AS full_name,
    LOWER(TRIM(email)) AS email, TRIM(city) AS city, UPPER(TRIM(state)) AS state
FROM workspace.bronze.customers WHERE customer_id IS NOT NULL;

CREATE OR REPLACE TABLE workspace.silver.products AS
SELECT DISTINCT product_id, TRIM(product_name) AS product_name,
    TRIM(category) AS category, TRIM(brand) AS brand, CAST(price AS DECIMAL(12,2)) AS price
FROM workspace.bronze.products WHERE product_id IS NOT NULL;

CREATE OR REPLACE TABLE workspace.silver.regions AS
SELECT DISTINCT region_id, TRIM(region) AS region
FROM workspace.bronze.regions WHERE region_id IS NOT NULL;
