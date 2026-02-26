USE WAREHOUSE COMPUTE_WH;
USE DATABASE SNOWFLAKE_LEARNING_DB;

--ADDING TRANSACTION DATE TO SILVER FACT

CREATE OR REPLACE TABLE SILVER.STORE_SALES_ENRICHED AS
SELECT
    ss.ss_customer_sk,
    ss.ss_store_sk,
    ss.ss_sales_price,
    d.d_date AS transaction_date
FROM SILVER.STORE_SALES_CLEAN ss
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM d
    ON ss.ss_sold_date_sk = d.d_date_sk;


SELECT * FROM SILVER.STORE_SALES_ENRICHED LIMIT 5;

DESC TABLE SILVER.STORE_SALES_ENRICHED;

--PROPER HISTORICAL JOIN QUERY

SELECT * 
FROM SILVER.STORE_SALES_ENRICHED 
LIMIT 100;

SELECT * FROM SILVER.CUSTOMER_DIM LIMIT 100;

SELECT
    f.transaction_date,
    d.state,
    SUM(f.ss_sales_price) AS total_sales
FROM SILVER.STORE_SALES_ENRICHED f
JOIN SILVER.CUSTOMER_DIM d
    ON f.ss_customer_sk = d.CUSTOMER_ID
WHERE f.transaction_date >= d.start_date
GROUP BY
    f.transaction_date,
    d.state
ORDER BY
    f.transaction_date;


INSERT INTO SILVER.FACT_STAGE VALUES
(101, 10, 500, '2024-01-10'),
(101, 10, 700, '2021-01-10');

-- Load the facts with surrogate keys

DROP TABLE IF EXISTS SILVER.CUSTOMER_DIM;

CREATE OR REPLACE TABLE SILVER.CUSTOMER_DIM (
    customer_sk NUMBER AUTOINCREMENT,
    customer_id NUMBER,
    state STRING,
    start_date DATE,
    end_date DATE,
    is_current STRING
);

INSERT INTO SILVER.CUSTOMER_DIM
(customer_id, state, start_date, end_date, is_current)
SELECT
    c.c_customer_sk,
    ca.ca_state,
    '1900-01-01',
    NULL,
    'Y'
FROM SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER c
JOIN SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.CUSTOMER_ADDRESS ca
    ON c.c_current_addr_sk = ca.ca_address_sk;

   CREATE OR REPLACE TABLE SILVER.FACT_SALES AS
SELECT
    d.customer_sk,
    f.ss_store_sk AS store_id,
    f.ss_sales_price AS sales_price,
    f.transaction_date
FROM SILVER.STORE_SALES_ENRICHED f
JOIN SILVER.CUSTOMER_DIM d
    ON f.ss_customer_sk = d.customer_id
    AND f.transaction_date >= d.start_date
    AND (f.transaction_date < d.end_date OR d.end_date IS NULL);

    SELECT * FROM SILVER.FACT_SALES;