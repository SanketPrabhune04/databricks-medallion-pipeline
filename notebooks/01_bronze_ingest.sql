-- =====================================================================
-- 01 · BRONZE — Raw ingestion
-- Loads source Parquet files as-is, adds audit/lineage columns.
-- Bronze is append-friendly and immutable: the raw safety net.
-- =====================================================================

-- Schemas + control (watermark) table -------------------------------
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
CREATE SCHEMA IF NOT EXISTS workspace.control;

CREATE TABLE IF NOT EXISTS workspace.control.batch_log (
    table_name STRING, batch_id STRING, loaded_at TIMESTAMP, row_count BIGINT
);

-- Bronze loads (raw + audit columns) --------------------------------
CREATE OR REPLACE TABLE workspace.bronze.customers AS
SELECT *, 'batch_001_first' AS _batch_id, current_timestamp() AS _ingested_at,
       _metadata.file_path AS _source_file
FROM read_files('/Volumes/workspace/default/source/customer_first.parquet', format => 'parquet');

CREATE OR REPLACE TABLE workspace.bronze.products AS
SELECT *, 'batch_001_first' AS _batch_id, current_timestamp() AS _ingested_at,
       _metadata.file_path AS _source_file
FROM read_files('/Volumes/workspace/default/source/products_first.parquet', format => 'parquet');

CREATE OR REPLACE TABLE workspace.bronze.regions AS
SELECT *, 'batch_001_first' AS _batch_id, current_timestamp() AS _ingested_at,
       _metadata.file_path AS _source_file
FROM read_files('/Volumes/workspace/default/source/regions.parquet', format => 'parquet');

CREATE OR REPLACE TABLE workspace.bronze.orders AS
SELECT *, 'batch_001_first' AS _batch_id, current_timestamp() AS _ingested_at,
       _metadata.file_path AS _source_file
FROM read_files('/Volumes/workspace/default/source/orders_first.parquet', format => 'parquet');

-- Incremental orders (batch 2) — idempotent via control-table guard -
INSERT INTO workspace.bronze.orders
    (order_id, customer_id, product_id, order_date, quantity, total_amount,
     _batch_id, _ingested_at, _source_file)
SELECT order_id, customer_id, product_id, order_date, quantity, total_amount,
       'batch_002_second', current_timestamp(), 'incremental_load'
FROM read_files('/Volumes/workspace/default/source/orders_second.parquet', format => 'parquet')
WHERE NOT EXISTS (SELECT 1 FROM workspace.control.batch_log
                  WHERE table_name='bronze.orders' AND batch_id='batch_002_second');

INSERT INTO workspace.control.batch_log
SELECT 'bronze.orders','batch_002_second',current_timestamp(),
       (SELECT COUNT(*) FROM workspace.bronze.orders WHERE _batch_id='batch_002_second')
WHERE NOT EXISTS (SELECT 1 FROM workspace.control.batch_log
                  WHERE table_name='bronze.orders' AND batch_id='batch_002_second');
