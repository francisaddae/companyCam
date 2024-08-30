-- Initianl data cleaning
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
SELECT *
FROM clean_data