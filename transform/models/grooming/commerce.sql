-- Initial data cleaning
WITH clean_data AS(
    SELECT
        COALESCE(transaction_id, 'TX9999') AS transaction_id,
        COALESCE(customer_id, 'C999') AS customer_id,
        COALESCE(product_id, 'P9999') AS product_id,
        COALESCE(date, CURRENT_DATE() + INTERVAL '10 YEARS') AS date,
        COALESCE(quantity, 0) AS quantity,
        COALESCE(price, 0.00) AS price,
        COALESCE(category, 'Unknown') AS category,
        COALESCE(payment_method, 'Unknown') AS payment_method,
        COALESCE(region, 'Unknown') AS region
    FROM {{ source('data_lake', 'ecommerce')}}
)
-- Dropping duplicates
/*
Based on this query, dropping there are a few records with identical row informations.
These needs to dropped to enforce data integrity and quality.

WITH dupes as (
    SELECT
        transaction_id, count(*)
    FROM default.ecommerce
    GROUP BY 1
    HAVING COUNT(*) > 1
)
SELECT *
FROM default.ecommerce
WHERE transaction_id IN (
    SELECT transaction_id from dupes)
ORDER BY transaction_id;

*/
, dropped_dupes AS (
    SELECT
        DISTINCT transaction_id,
        customer_id,
        product_id,
        date,
        quantity,
        price,
        category,
        payment_method,
        region
    FROM clean_data
)
SELECT *
FROM dropped_dupes