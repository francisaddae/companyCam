--Modeling Customers table
WITH customer_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY customer_id) AS customers_id,
        customer_id
    FROM {{ref( 'commerce') }}
    GROUP BY customer_id
)
SELECT *
FROM customer_table