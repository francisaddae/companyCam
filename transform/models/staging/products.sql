--Modeling Products table
WITH product_table AS(
    SELECT
        ROW_NUMBER() OVER(ORDER BY product_id) AS products_id,
        product_id
    FROM {{ref( 'commerce') }}
    GROUP BY product_id
)
SELECT *
FROM product_table