--Modeling Transaction table
WITH transactions_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY transaction_id) AS transactions_id,
        transaction_id,
        date,
        quantity,
        price,
        DENSE_RANK() OVER(ORDER BY customer_id) AS customers_id,
        DENSE_RANK() OVER(ORDER BY product_id) AS products_id,
        DENSE_RANK() OVER(ORDER BY region) AS region_id,
        DENSE_RANK() OVER(ORDER BY payment_method) AS payment_method_id,
        DENSE_RANK() OVER(ORDER BY category) AS category_id
    FROM {{ref( 'commerce') }}
    GROUP BY
        transaction_id,
        date,
        quantity,
        price,
        customer_id,
        product_id,
        region,
        payment_method,
        category
)
SELECT *
FROM transactions_table