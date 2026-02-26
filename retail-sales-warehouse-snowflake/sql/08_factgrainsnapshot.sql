
USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE OR REPLACE TABLE gold.monthy_sales as
SELECT
    date_trunc('month',transaction_date) AS sales_month,
    SUM(sales_price) AS total_sales
FROM SILVER.FACT_SALES
GROUP BY sales_month;