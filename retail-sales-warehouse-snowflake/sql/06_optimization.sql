USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

-- -----------------------------------------------------
-- 1 )Partition Pruning Example
-- -----------------------------------------------------
-- Filtering on transaction_date improves pruning

SELECT
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
WHERE transaction_date BETWEEN '2021-01-01' AND '2021-12-31';


-- -----------------------------------------------------
--2) Pre-Aggregation Strategy (Move Compute to Build Time)
-- -----------------------------------------------------

CREATE OR REPLACE TABLE GOLD.SALES_PREAGG_BY_STORE AS
SELECT
    store_id,
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY store_id;


-- -----------------------------------------------------
-- 3) Clustering Key (Improves Range Queries)
-- -----------------------------------------------------

-- Optional optimization for large tables
-- Helps when frequently filtering by transaction_date

ALTER TABLE SILVER.FACT_SALES
CLUSTER BY (transaction_date);


-- -----------------------------------------------------
-- 4) Avoid SELECT * (Reduce Scan Volume)
-- -----------------------------------------------------

-- Bad Practice:
-- SELECT * FROM SILVER.FACT_SALES;

-- Better Practice:
SELECT
    store_id,
    sales_price
FROM SILVER.FACT_SALES;


-- -----------------------------------------------------
-- 5) Query Profile Inspection
-- -----------------------------------------------------

-- Run a heavy query and inspect Query Profile in Snowflake UI
SELECT
    DATE_TRUNC('month', transaction_date) AS sales_month,
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY sales_month;


-- -----------------------------------------------------
-- 6 Compare Gold vs Silver Performance
-- -----------------------------------------------------

-- Silver (heavier computation)
SELECT SUM(sales_price)
FROM SILVER.FACT_SALES;

-- Gold (pre-aggregated, faster)
SELECT SUM(total_sales)
FROM GOLD.SALES_BY_YEAR;